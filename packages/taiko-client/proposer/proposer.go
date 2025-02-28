package proposer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/txmgr"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/urfave/cli/v2"

	"github.com/taikoxyz/taiko-mono/packages/taiko-client/bindings"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/bindings/encoding"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/internal/metrics"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/pkg/config"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/pkg/rpc"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/pkg/utils"
	builder "github.com/taikoxyz/taiko-mono/packages/taiko-client/proposer/transaction_builder"
)

// Proposer keep proposing new transactions from L2 execution engine's tx pool at a fixed interval.
type Proposer struct {
	// configurations
	*Config

	// RPC clients
	rpc *rpc.Client

	// Private keys and account addresses
	proposerAddress common.Address

	proposingTimer *time.Timer

	// Transaction builder
	txBuilder builder.ProposeBlockTransactionBuilder

	// Protocol configurations
	protocolConfigs *bindings.TaikoDataConfig

	chainConfig *config.ChainConfig

	lastProposedAt time.Time
	totalEpochs    uint64

	txmgrSelector *utils.TxMgrSelector

	ctx context.Context
	wg  sync.WaitGroup

	// Mutex for concurrent access
	proposalMutex sync.Mutex

	// Maps to store proposal data
	proposedTxHashes map[string]bool
}

const (
	GenesisTime int64 = 1606824023 // Ethereum Beacon Chain Genesis Time (Dec 1, 2020)
	// GenesisTime          int64   = 1695902400 // Genesis Time for Holesky
	SlotTime             float64 = 12.0       // Each slot lasts 12 seconds
	TimeGapToPropose     float64 = 3.0        // Time gap to propose in seconds
	MaxBlobSpaceSize             = 128 * 1024 // Define the maximum blob space size as 128 KB
	BaseFeePctToProposer         = 75         // The percentage of the base fee that the proposer will receive
	DefaultL1GasSpent    int64   = 150000     // The amount of gas spent on L1
	BlockGasDiscount             = 40         // The percentage of the gas discount
)

// InitFromCli initializes the given proposer instance based on the command line flags.
func (p *Proposer) InitFromCli(ctx context.Context, c *cli.Context) error {
	cfg, err := NewConfigFromCliContext(c)
	if err != nil {
		return err
	}

	return p.InitFromConfig(ctx, cfg, nil, nil)
}

// InitFromConfig initializes the proposer instance based on the given configurations.
func (p *Proposer) InitFromConfig(
	ctx context.Context, cfg *Config,
	txMgr *txmgr.SimpleTxManager,
	privateTxMgr *txmgr.SimpleTxManager,
) (err error) {
	p.proposerAddress = crypto.PubkeyToAddress(cfg.L1ProposerPrivKey.PublicKey)
	p.ctx = ctx
	p.Config = cfg
	p.lastProposedAt = time.Now()

	// RPC clients
	if p.rpc, err = rpc.NewClient(p.ctx, cfg.ClientConfig); err != nil {
		return fmt.Errorf("initialize rpc clients error: %w", err)
	}

	// Protocol configs
	protocolConfigs, err := rpc.GetProtocolConfigs(p.rpc.TaikoL1, &bind.CallOpts{Context: p.ctx})
	if err != nil {
		return fmt.Errorf("failed to get protocol configs: %w", err)
	}
	p.protocolConfigs = &protocolConfigs
	log.Info("Protocol configs", "configs", p.protocolConfigs)

	if txMgr == nil {
		if txMgr, err = txmgr.NewSimpleTxManager(
			"proposer",
			log.Root(),
			&metrics.TxMgrMetrics,
			*cfg.TxmgrConfigs,
		); err != nil {
			return err
		}
	}

	if privateTxMgr == nil && cfg.PrivateTxmgrConfigs != nil && len(cfg.PrivateTxmgrConfigs.L1RPCURL) > 0 {
		if privateTxMgr, err = txmgr.NewSimpleTxManager(
			"privateMempoolProposer",
			log.Root(),
			&metrics.TxMgrMetrics,
			*cfg.PrivateTxmgrConfigs,
		); err != nil {
			return err
		}
	}

	p.txmgrSelector = utils.NewTxMgrSelector(txMgr, privateTxMgr, nil)
	p.chainConfig = config.NewChainConfig(p.protocolConfigs)
	p.txBuilder = builder.NewBuilderWithFallback(
		p.rpc,
		p.L1ProposerPrivKey,
		cfg.L2SuggestedFeeRecipient,
		cfg.TaikoL1Address,
		cfg.ProverSetAddress,
		cfg.ProposeBlockTxGasLimit,
		p.chainConfig,
		p.txmgrSelector,
		cfg.RevertProtectionEnabled,
		cfg.BlobAllowed,
		cfg.FallbackToCalldata,
	)

	// Initialize maps
	p.proposedTxHashes = make(map[string]bool)

	return nil
}

// Start starts the proposer's main loop.
func (p *Proposer) Start() error {
	p.wg.Add(1)
	go func() {
		// Start the event loop
		p.eventLoop()
	}()
	return nil
}

// eventLoop starts the main loop of Taiko proposer.
func (p *Proposer) eventLoop() {
	defer func() {
		p.proposingTimer.Stop()
		p.wg.Done()
	}()

	for {
		log.Info("Event loop started", "Epochs", p.totalEpochs)
		p.updateProposingTicker()

		select {
		case <-p.ctx.Done():
			return
		// proposing interval timer has been reached
		case <-p.proposingTimer.C:
			metrics.ProposerProposeEpochCounter.Add(1)
			p.totalEpochs++

			// Attempt a proposing operation
			if err := p.ProposeOp(p.ctx); err != nil {
				log.Error("Proposing operation error", "error", err)
				continue
			}

		}
	}
}

// Close closes the proposer instance.
func (p *Proposer) Close(_ context.Context) {
	p.wg.Wait()
}

// fetchPoolContent fetches the transaction pool content from L2 execution engine.
func (p *Proposer) fetchPoolContent(filterPoolContent bool) ([]types.Transactions, error) {
	var (
		minTip  = p.MinTip
		startAt = time.Now()
	)
	// If `--epoch.allowZeroInterval` flag is set, allow proposing zero tip transactions once when
	// the total epochs number is divisible by the flag value.
	if p.AllowZeroInterval > 0 && p.totalEpochs%p.AllowZeroInterval == 0 {
		minTip = 0
	}

	metrics.ProposerPoolContentFetchTime.Set(time.Since(startAt).Seconds())

	// Fetch the pool content.
	preBuiltTxList, err := p.rpc.GetPoolContent(
		p.ctx,
		p.proposerAddress,
		p.protocolConfigs.BlockMaxGasLimit,
		rpc.BlockMaxTxListBytes,
		p.LocalAddresses,
		p.MaxProposedTxListsPerEpoch,
		minTip,
		p.chainConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transaction pool content: %w", err)
	}

	txLists := []types.Transactions{}

	for i, txs := range preBuiltTxList {
		// Filter the pool content if the filterPoolContent flag is set.
		if txs.EstimatedGasUsed < p.MinGasUsed && txs.BytesLength < p.MinTxListBytes && filterPoolContent {
			log.Error(
				"Pool content skipped",
				"index", i,
				"estimatedGasUsed", txs.EstimatedGasUsed,
				"minGasUsed", p.MinGasUsed,
				"bytesLength", txs.BytesLength,
				"minBytesLength", p.MinTxListBytes,
			)
			break
		}

		// Here we check if the BytesLength of the txList is greater than the maxBlobSpaceSize
		if txs.BytesLength > MaxBlobSpaceSize {
			sort.SliceStable(txs.TxList, func(i, j int) bool {
				cmp := txs.TxList[i].GasTipCap().Cmp(txs.TxList[j].GasTipCap())
				if cmp != 0 {
					return cmp > 0
				}
				return txs.TxList[i].Gas() > txs.TxList[j].Gas()
			})
			var newTxList types.Transactions
			var currentBlobSize uint64

			for _, tx := range txs.TxList {
				hasProposed := p.hasBeenProposed(tx.Hash())
				if hasProposed {
					log.Info(
						"Not including previously proposed transaction since blob space is limited",
						"txHash", tx.Hash().Hex())
					continue
				}

				txSize := uint64(len(tx.Data()))
				if currentBlobSize+txSize > MaxBlobSpaceSize {
					break
				}
				newTxList = append(newTxList, tx)
				currentBlobSize += txSize
			}

			txLists = append(txLists, newTxList)
		} else {
			// The txList is less than the MaxBlobSpaceSize, we can add it to the txLists
			txLists = append(txLists, txs.TxList)
		}
	}
	// If the pool content is empty and the checkPoolContent flag is not set, return an empty list.
	if !filterPoolContent && len(txLists) == 0 {
		log.Info(
			"Pool content is empty, proposing an empty block",
			"lastProposedAt", p.lastProposedAt,
			"minProposingInternal", p.MinProposingInternal,
		)
		txLists = append(txLists, types.Transactions{})
	}

	// If LocalAddressesOnly is set, filter the transactions by the local addresses.
	if p.LocalAddressesOnly {
		var (
			localTxsLists []types.Transactions
			signer        = types.LatestSignerForChainID(p.rpc.L2.ChainID)
		)
		for _, txs := range txLists {
			var filtered types.Transactions
			for _, tx := range txs {
				sender, err := types.Sender(signer, tx)
				if err != nil {
					return nil, err
				}

				for _, localAddress := range p.LocalAddresses {
					if sender == localAddress {
						filtered = append(filtered, tx)
					}
				}
			}

			if filtered.Len() != 0 {
				localTxsLists = append(localTxsLists, filtered)
			}
		}
		txLists = localTxsLists
	}

	return txLists, nil
}

// ProposeOp performs a proposing operation, fetching transactions
// from L2 execution engine's tx pool, splitting them by proposing constraints,
// and then proposing them to TaikoL1 contract.
func (p *Proposer) ProposeOp(ctx context.Context) error {
	startTime := time.Now()
	defer func() {
		totalDuration := time.Since(startTime)
		metrics.ProposerTotalDuration.Set(totalDuration.Seconds())
		log.Info("ProposeOp total duration", "duration", totalDuration)
	}()

	// Check if it's time to propose unfiltered pool content.
	filterPoolContent := time.Now().Before(p.lastProposedAt.Add(p.MinProposingInternal))

	// Wait until L2 execution engine is synced at first.
	if err := p.rpc.WaitTillL2ExecutionEngineSynced(ctx); err != nil {
		return fmt.Errorf("failed to wait until L2 execution engine synced: %w", err)
	}

	log.Info(
		"Start fetching L2 execution engine's transaction pool content",
		"filterPoolContent", filterPoolContent,
		"lastProposedAt", p.lastProposedAt,
	)

	fetchStartTime := time.Now()
	// Fetch pending L2 transactions from mempool.
	txLists, err := p.fetchPoolContent(filterPoolContent)
	fetchDuration := time.Since(fetchStartTime)
	metrics.ProposerFetchPoolContentDuration.Set(fetchDuration.Seconds())
	log.Info("Fetch pool content duration", "duration", fetchDuration, "numOfTxs", len(txLists[0]))

	if err != nil {
		return err
	}

	// Filter out transactions that have been proposed within the last 24 hours.
	var filteredTxLists []types.Transactions
	skippedTxs := 0
	for _, txs := range txLists {
		var filteredTxs types.Transactions
		for _, tx := range txs {
			hasProposed := p.hasBeenProposed(tx.Hash())
			if hasProposed {
				skippedTxs++
				continue
			}
			filteredTxs = append(filteredTxs, tx)
		}
		if filteredTxs.Len() > 0 {
			filteredTxLists = append(filteredTxLists, filteredTxs)
		}
	}
	log.Info("Skipped transactions", "count", skippedTxs)
	txLists = filteredTxLists

	//Update the lastProposedAt to the current time before getting to the last part
	p.lastProposedAt = time.Now()

	// Retrieve the L2 base fee and L2 block number
	l2BaseFee, err := p.rpc.GetL2BaseFee(ctx, p.chainConfig)
	if err != nil {
		log.Error("failed to get L2 base fee:", "error", err)
		return nil
	}

	l2BlockNum, err := p.rpc.L2.BlockNumber(ctx)
	if err != nil {
		log.Error("Failed to fetch L2 block number", "error", err)
	}

	totalEarnings := new(big.Int).SetUint64(0)
	totalNumberOfTxs := 0
	for _, txs := range txLists {
		for _, tx := range txs {
			discountedGas := tx.Gas()
			if tx.Gas() > 30000 {
				discountedGas = tx.Gas() * BlockGasDiscount / 100 // Apply gas discount
			}
			totalEarnings.Add(totalEarnings,
				new(big.Int).SetUint64((((l2BaseFee.Uint64()*BaseFeePctToProposer)/100)+tx.GasTipCap().Uint64())*discountedGas))

			totalNumberOfTxs++
		}
		if err != nil {
			log.Error("Failed to get current base fee", "error", err)
			continue
		}
	}

	// Get the L1 gas fee and blob fee
	FeeHistory, err := p.rpc.L1.FeeHistory(ctx, 1, nil, []float64{90})
	if err != nil {
		log.Error("Failed to get fee history", "error", err)
		return err
	}

	// Use the Add method to sum the base fee and reward
	l1Cost := new(big.Int).Set(FeeHistory.BaseFee[1]) // Create a new big.Int and set it to BaseFee[1]
	l1Cost.Add(l1Cost, FeeHistory.Reward[0][0])       // Add the priority fee to the base fee
	l1Cost.Mul(l1Cost, big.NewInt(DefaultL1GasSpent)) // Multiply by 150000

	// Save proposal data using metrics
	l1BlockNum, err := p.rpc.L1.BlockNumber(p.ctx)
	if err != nil {
		log.Error("Failed to fetch L1 block number", "error", err)
	} else {
		// Record proposal data using metrics
		// Add to metrics engine
		// When you have new data for a block
		metrics.UpdateBlockMetrics(
			int64(l1BlockNum),                                    // L1 block number
			float64(l1Cost.Int64()),                              // L1 cost as float64
			float64(totalEarnings.Int64()),                       // total earnings as float64
			SlotTime-p.getRemainingTimeLeftInL1Block(time.Now()), // proposed at timestamp
			int64(totalNumberOfTxs),                              // total number of txs
			float64(FeeHistory.BaseFee[1].Int64()),               // L1 base fee
			float64(FeeHistory.Reward[0][0].Int64()),             // L1 priority fee
			float64(l2BaseFee.Int64()),
			int64(l2BlockNum), // L2 block number
		)
	}

	if totalEarnings.Cmp(l1Cost) > 0 {
		log.Info("Expected profit, proposing transactions",
			"profit", utils.WeiToEther(totalEarnings.Sub(totalEarnings, l1Cost)))
		// Increment proposal counter
		metrics.ProposerProposeEpochCounter.Add(1)

		// Propose the transactions lists.
		proposeStartTime := time.Now()
		err = p.ProposeTxLists(ctx, txLists)

		proposeDuration := time.Since(proposeStartTime)
		metrics.ProposerProposeTxListsDuration.Set(proposeDuration.Seconds())
		log.Info("ProposeTxLists duration", "duration", proposeDuration)

		// Mark all proposed transactions
		for _, txs := range txLists {
			for _, tx := range txs {
				p.markAsProposed(tx.Hash())
			}
		}

	} else {
		log.Info("L1 cost is greater than total earnings, skipping proposal",
			"Deficit",
			utils.WeiToEther(l1Cost.Sub(l1Cost, totalEarnings)))
	}

	return nil
}

// ProposeTxList proposes the given transactions lists to TaikoL1 smart contract.
func (p *Proposer) ProposeTxLists(ctx context.Context, txLists []types.Transactions) error {
	// If the current L2 chain is after ontake fork, batch propose all L2 transactions lists.
	if err := p.ProposeTxListOntake(ctx, txLists); err != nil {
		return err
	}
	return nil
}

// ProposeTxListOntake proposes the given transactions lists to TaikoL1 smart contract.
func (p *Proposer) ProposeTxListOntake(
	ctx context.Context,
	txLists []types.Transactions,
) error {
	var (
		proverAddress     = p.proposerAddress
		txListsBytesArray [][]byte
		txNums            []int
		totalTxs          int
	)
	for _, txs := range txLists {
		txListBytes, err := rlp.EncodeToBytes(txs)

		if err != nil {
			return fmt.Errorf("failed to encode transactions: %w", err)
		}

		compressedTxListBytes, err := utils.Compress(txListBytes)

		if err != nil {
			return err
		}

		txListsBytesArray = append(txListsBytesArray, compressedTxListBytes)
		txNums = append(txNums, len(txs))
		totalTxs += len(txs)
	}

	if p.Config.ClientConfig.ProverSetAddress != rpc.ZeroAddress {
		proverAddress = p.Config.ClientConfig.ProverSetAddress
	}

	ok, err := rpc.CheckProverBalance(
		ctx,
		p.rpc,
		proverAddress,
		p.TaikoL1Address,
		new(big.Int).Mul(p.protocolConfigs.LivenessBond, new(big.Int).SetUint64(uint64(len(txLists)))),
	)

	if err != nil {
		log.Warn("Failed to check prover balance", "error", err)
		return err
	}

	if !ok {
		return errors.New("insufficient prover balance")
	}

	txCandidate, err := p.txBuilder.BuildOntake(ctx, txListsBytesArray)
	if err != nil {
		log.Warn("Failed to build TaikoL1.proposeBlocksV2 transaction", "error", encoding.TryParsingCustomError(err))
		return err
	}

	if err := p.sendTx(ctx, txCandidate); err != nil {
		return err
	}

	log.Info("📝 Batch propose transactions succeeded", "txs", txNums)

	metrics.ProposerProposedTxListsCounter.Add(float64(len(txLists)))
	metrics.ProposerProposedTxsCounter.Add(float64(totalTxs))

	return nil
}

// updateProposingTicker updates the internal proposing timer.
func (p *Proposer) updateProposingTicker() {
	if p.proposingTimer != nil {
		p.proposingTimer.Stop()
	}

	// Calculate the time until the next target point in the L1 block cycle
	// This will be at SlotTime - TimeGapToPropose (9th second) of each block
	nextRunTime := p.calculateTimeToNextProposingPoint()

	log.Info("Scheduling next proposal",
		"timeUntilNextRun", nextRunTime.Seconds(),
		"targetSecondInBlock", SlotTime-TimeGapToPropose)

	p.proposingTimer = time.NewTimer(nextRunTime)
}

// calculateTimeToNextProposingPoint calculates the duration until we should run
// the next proposeOp, targeting exactly the (SlotTime - TimeGapToPropose) second
// of each L1 block.
func (p *Proposer) calculateTimeToNextProposingPoint() time.Duration {
	now := time.Now().UTC()
	currentTime := float64(now.UnixNano()) / 1e9 // Current time in seconds
	elapsedSinceGenesis := currentTime - float64(GenesisTime)

	// Calculate which slot we're in
	currentSlot := elapsedSinceGenesis / SlotTime

	// Calculate the start time of the current slot
	currentSlotStartTime := float64(GenesisTime) + (math.Floor(currentSlot) * SlotTime)

	// Calculate the target time within the slot (SlotTime - TimeGapToPropose seconds after slot start)
	targetPointInCurrentSlot := currentSlotStartTime + (SlotTime - TimeGapToPropose)

	// If we've already passed the target point in the current slot, aim for the next slot
	if currentTime >= targetPointInCurrentSlot {
		targetPointInCurrentSlot += SlotTime
	}

	// Calculate the duration until the target point
	waitDuration := targetPointInCurrentSlot - currentTime

	return time.Duration(waitDuration * float64(time.Second))
}

func (p *Proposer) getRemainingTimeLeftInL1Block(inputTime time.Time) (timeLeft float64) {
	currentTime := float64(inputTime.UTC().UnixNano()) / 1e9 // Get current UTC time in seconds (float64)
	elapsedTime := currentTime - float64(GenesisTime)

	slot := int64(elapsedTime / float64(SlotTime))               // Compute current slot
	timeLeft = SlotTime - (elapsedTime - float64(slot)*SlotTime) // Compute time left in slot

	return timeLeft
}

// sendTx is the internal function to send a transaction with a selected tx manager.
func (p *Proposer) sendTx(ctx context.Context, txCandidate *txmgr.TxCandidate) error {
	txMgr, isPrivate := p.txmgrSelector.Select()
	receipt, err := txMgr.Send(ctx, *txCandidate)
	if err != nil {
		log.Warn(
			"Failed to send TaikoL1.proposeBlock / TaikoL1.proposeBlocksV2 transaction by tx manager",
			"isPrivateMempool", isPrivate,
			"error", encoding.TryParsingCustomError(err),
		)
		if isPrivate {
			p.txmgrSelector.RecordPrivateTxMgrFailed()
		}
		return err
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		return fmt.Errorf("failed to propose block: %s", receipt.TxHash.Hex())
	}
	return nil
}

// Name returns the application name.
func (p *Proposer) Name() string {
	return "proposer"
}

func (p *Proposer) hasBeenProposed(txHash common.Hash) bool {
	p.proposalMutex.Lock()
	defer p.proposalMutex.Unlock()
	_, exists := p.proposedTxHashes[txHash.Hex()]
	return exists
}

func (p *Proposer) markAsProposed(txHash common.Hash) {
	p.proposalMutex.Lock()
	defer p.proposalMutex.Unlock()
	p.proposedTxHashes[txHash.Hex()] = true

	// Schedule removal after 1 hour
	go func() {
		time.Sleep(1 * time.Hour) // Sleep for 1 hour
		p.proposalMutex.Lock()
		defer p.proposalMutex.Unlock()
		delete(p.proposedTxHashes, txHash.Hex())
	}()
}
