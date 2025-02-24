package proposer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/redis/go-redis/v9"
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

	redisClient *redis.Client
}

const (
	//GenesisTime int64 = 1606824023 // Ethereum Beacon Chain Genesis Time (Dec 1, 2020)
	GenesisTime          int64   = 1695902400 // Genesis Time for Holesky
	SlotTime             float64 = 12.0       // Each slot lasts 12 seconds
	TimeGapToPropose     float64 = 3.0        // Time gap to propose in seconds
	MaxBlobSpaceSize             = 128 * 1024 // Define the maximum blob space size as 128 KB
	BaseFeePctToProposer         = 75         // The percentage of the base fee that the proposer will receive
	DefaultL1GasSpent    int64   = 150000     // The amount of gas spent on L1
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

	p.redisClient = redis.NewClient(&redis.Options{
		Addr:     cfg.RedisConfig.Address,
		Password: cfg.RedisConfig.Password,
		DB:       0, //Default DB
	})
	log.Info("Redis instantiated successfully", "address", cfg.RedisConfig.Address)
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

			//Update the L1 block number in Redis
			//Before proposing, we check if the L1 block number is the same as the last proposed block number
			//If it is, we skip the proposal
			//If it is not, we propose the transactions
			l1BlockNumber, err := p.rpc.L1.BlockNumber(p.ctx)
			if err != nil {
				log.Error("Failed to fetch L1 block number", "error", err)
				return
			}

			p.recordL1BlockNumber(l1BlockNumber)
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
				hasProposed, err := p.hasBeenProposed(tx.Hash())
				if err != nil {
					log.Error("Error checking if transaction has been proposed", "txHash", tx.Hash().Hex(), "error", err)
					continue
				}
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
	for _, txs := range txLists {
		var filteredTxs types.Transactions
		for _, tx := range txs {
			hasProposed, err := p.hasBeenProposed(tx.Hash())
			if err != nil {
				log.Error("Error checking if transaction has been proposed", "txHash", tx.Hash().Hex(), "error", err)
				continue
			}
			if hasProposed {
				log.Info("Skipping previously proposed transaction", "txHash", tx.Hash().Hex())
				continue
			}
			filteredTxs = append(filteredTxs, tx)
		}
		if filteredTxs.Len() > 0 {
			filteredTxLists = append(filteredTxLists, filteredTxs)
		}
	}
	txLists = filteredTxLists

	//Update the lastProposedAt to the current time before getting to the last part
	p.lastProposedAt = time.Now()

	// Retrieve the L2 base fee
	baseFee, err := p.rpc.GetL2BaseFee(ctx, p.chainConfig)
	if err != nil {
		log.Error("failed to get L2 base fee:", "error", err)
		return nil
	}

	totalEarnings := new(big.Int).SetUint64(0)
	totalNumberOfTxs := 0
	for _, txs := range txLists {
		for _, tx := range txs {
			if tx.Gas() < 30000 {
				discountedGas := tx.Gas() * 60 / 100 // Apply 40% discount
				totalEarnings.Add(totalEarnings, new(big.Int).SetUint64(((baseFee.Uint64()*BaseFeePctToProposer)/100+tx.GasTipCap().Uint64())*discountedGas))
			} else {
				totalEarnings.Add(totalEarnings, new(big.Int).SetUint64(((baseFee.Uint64()*BaseFeePctToProposer)/100+tx.GasTipCap().Uint64())*tx.Gas()))
			}
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

	log.Info("Earnings and L1 cost",
		"totalEarnings",
		utils.WeiToEther(totalEarnings),
		"l1Cost",
		utils.WeiToEther(l1Cost),
	)

	// Save proposal data to Redis
	l2blockNumber, err := p.rpc.L2.BlockNumber(context.Background())
	if err != nil {
		log.Error("Failed to get L2 block number", "error", err)
		l2blockNumber = 0
	}

	err = p.saveProposalData(
		p.getL1BlockNumber(),
		l2blockNumber,
		l1Cost,
		totalEarnings,
		totalNumberOfTxs,
	)
	if err != nil {
		log.Error("Failed to save proposal data in Redis", "error", err)
		return err
	}

	if totalEarnings.Cmp(l1Cost) > 0 {
		log.Info("Expected profit, proposing transactions",
			"profit", utils.WeiToEther(totalEarnings.Sub(totalEarnings, l1Cost)))
		// Propose the transactions lists.
		proposeStartTime := time.Now()
		err = p.ProposeTxLists(ctx, txLists)

		proposeDuration := time.Since(proposeStartTime)
		metrics.ProposerProposeTxListsDuration.Set(proposeDuration.Seconds())
		log.Info("ProposeTxLists duration", "duration", proposeDuration)

		// Mark all proposed transactions
		for _, txs := range txLists {
			for _, tx := range txs {
				err := p.markAsProposed(tx.Hash())
				if err != nil {
					log.Warn("Failed to mark transaction as proposed", "txHash", tx.Hash().Hex(), "error", err)
					return err
				}
			}
		}

	} else {
		log.Info("L1 cost is greater than total earnings, skipping proposal",
			"Deficit",
			utils.WeiToEther(l1Cost.Sub(l1Cost, totalEarnings)))
		return nil
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

	if p.totalEpochs == 0 {
		// Calculate the initial wait time to align with the L1 block
		// Get the time left in the current L1 block slot
		timeLeftInSlot := p.getRemainingTimeLeftInL1Block()
		initialWaitTime := p.calculateInitialWaitTime(timeLeftInSlot)
		log.Info("We will sleep till our clock get aligned with the L1 block",
			"initialWaitTime", initialWaitTime)
		p.proposingTimer = time.NewTimer(initialWaitTime)
	} else {
		// we will wakeup the proposer every slottime
		slotDuration := time.Duration(SlotTime) * time.Second

		// Set the proposing timer
		p.proposingTimer = time.NewTimer(slotDuration)
	}
}

func (p *Proposer) getRemainingTimeLeftInL1Block() (timeLeft float64) {
	currentTime := float64(time.Now().UTC().UnixNano()) / 1e9 // Get current UTC time in seconds (float64)
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

func (p *Proposer) calculateInitialWaitTime(timeLeftInSlot float64) time.Duration {
	// If the time left in the slot is less than TimeGapToPropose, wait for the next slot
	if timeLeftInSlot < TimeGapToPropose {
		timeLeftInSlot += float64(SlotTime)
	}

	// Calculate the initial wait time to align with the next L1 block
	waitTime := timeLeftInSlot - TimeGapToPropose
	if waitTime < 0 {
		waitTime = 0 // Ensure wait time is not negative
	}

	return time.Duration(waitTime * float64(time.Second))
}

// hasBeenProposed checks if the transaction hash exists in Redis.
func (p *Proposer) hasBeenProposed(txHash common.Hash) (bool, error) {
	exists, err := p.redisClient.Exists(p.ctx, txHash.Hex()).Result()
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

// markAsProposed stores the transaction hash in Redis with a 24-hour expiration.
func (p *Proposer) markAsProposed(txHash common.Hash) error {
	err := p.redisClient.Set(p.ctx, txHash.Hex(), 1, 24*time.Hour).Err()
	if err != nil {
		return err
	}
	return nil
}

func (p *Proposer) recordL1BlockNumber(l1BlockNumber uint64) {
	err := p.redisClient.Set(p.ctx, "l1_block_number", l1BlockNumber, 24*time.Hour).Err()
	if err != nil {
		log.Error("Failed to record L1 block number in Redis", "error", err)
		return
	}
}

func (p *Proposer) getL1BlockNumber() string {
	l1BlockNumber, err := p.redisClient.Get(p.ctx, "l1_block_number").Result()
	if err != nil {
		return "0"
	}
	return l1BlockNumber
}

// saveProposalData saves the relevant proposal data to Redis.
func (p *Proposer) saveProposalData(
	l1BlockNumber string,
	l2BlockNumber uint64,
	l1Cost *big.Int,
	totalEarnings *big.Int,
	totalNumberOfTxs int) error {
	// Create a proposal data structure
	proposalData := map[string]interface{}{
		"l1_block_number":     l1BlockNumber,
		"l1_cost":             l1Cost.String(),
		"total_earnings":      totalEarnings.String(),
		"last_proposed_at":    p.lastProposedAt.Format(time.RFC3339), // Store as ISO 8601 string
		"total_number_of_txs": totalNumberOfTxs,
	}

	// Marshal the proposal data to JSON
	proposalDataJSON, err := json.Marshal(proposalData)
	if err != nil {
		log.Error("Failed to marshal proposal data to JSON", "error", err)
		return err
	}

	// Save the proposal data in the format [l2_block_number] : [all the remaining data]
	err = p.redisClient.Set(p.ctx, fmt.Sprintf("%d", l2BlockNumber), proposalDataJSON, 24*time.Hour).Err()
	if err != nil {
		log.Error("Failed to save proposal data in Redis", "l2_block_number", l2BlockNumber, "error", err)
		return err
	}
	return nil
}
