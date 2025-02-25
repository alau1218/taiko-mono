package proposer

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/txmgr"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/miner"
	"github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	// Alias the standard library math package

	"github.com/taikoxyz/taiko-mono/packages/taiko-client/bindings"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/bindings/metadata"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/driver/chain_syncer/beaconsync"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/driver/chain_syncer/blob"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/driver/state"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/internal/testutils"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/pkg/jwt"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/pkg/rpc"
)

type ProposerTestSuite struct {
	testutils.ClientTestSuite
	s         *blob.Syncer
	p         *Proposer
	cancel    context.CancelFunc
	proposeCh chan struct{}
	wg        sync.WaitGroup
}

func (s *ProposerTestSuite) SetupTest() {
	s.ClientTestSuite.SetupTest()

	log.Info("Setting up state and syncer.")
	state2, err := state.New(context.Background(), s.RPCClient)
	s.Nil(err)

	syncer, err := blob.NewSyncer(
		context.Background(),
		s.RPCClient,
		state2,
		beaconsync.NewSyncProgressTracker(s.RPCClient.L2, 1*time.Hour),
		0,
		nil,
		nil,
	)
	s.Nil(err)
	s.s = syncer

	log.Info("Initializing proposer.")
	l1ProposerPrivKey, err := crypto.ToECDSA(common.FromHex(os.Getenv("L1_PROPOSER_PRIVATE_KEY")))
	s.Nil(err)

	p := new(Proposer)

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	jwtSecret, err := jwt.ParseSecretFromFile(os.Getenv("JWT_SECRET"))
	s.Nil(err)
	s.NotEmpty(jwtSecret)

	log.Info("Initializing proposer configuration.")
	s.Nil(p.InitFromConfig(ctx, &Config{
		ClientConfig: &rpc.ClientConfig{
			L1Endpoint:        os.Getenv("L1_WS"),
			L2Endpoint:        os.Getenv("L2_HTTP"),
			L2EngineEndpoint:  os.Getenv("L2_AUTH"),
			JwtSecret:         string(jwtSecret),
			TaikoL1Address:    common.HexToAddress(os.Getenv("TAIKO_L1")),
			TaikoL2Address:    common.HexToAddress(os.Getenv("TAIKO_L2")),
			TaikoTokenAddress: common.HexToAddress(os.Getenv("TAIKO_TOKEN")),
		},
		L1ProposerPrivKey:          l1ProposerPrivKey,
		L2SuggestedFeeRecipient:    common.HexToAddress(os.Getenv("L2_SUGGESTED_FEE_RECIPIENT")),
		MinProposingInternal:       0,
		MaxProposedTxListsPerEpoch: 1,
		ProposeBlockTxGasLimit:     10_000_000,
		FallbackToCalldata:         true,
		TxmgrConfigs: &txmgr.CLIConfig{
			L1RPCURL:                  os.Getenv("L1_WS"),
			NumConfirmations:          0,
			SafeAbortNonceTooLowCount: txmgr.DefaultBatcherFlagValues.SafeAbortNonceTooLowCount,
			PrivateKey:                common.Bytes2Hex(crypto.FromECDSA(l1ProposerPrivKey)),
			FeeLimitMultiplier:        txmgr.DefaultBatcherFlagValues.FeeLimitMultiplier,
			FeeLimitThresholdGwei:     txmgr.DefaultBatcherFlagValues.FeeLimitThresholdGwei,
			MinBaseFeeGwei:            txmgr.DefaultBatcherFlagValues.MinBaseFeeGwei,
			MinTipCapGwei:             txmgr.DefaultBatcherFlagValues.MinTipCapGwei,
			ResubmissionTimeout:       txmgr.DefaultBatcherFlagValues.ResubmissionTimeout,
			ReceiptQueryInterval:      1 * time.Second,
			NetworkTimeout:            txmgr.DefaultBatcherFlagValues.NetworkTimeout,
			TxSendTimeout:             txmgr.DefaultBatcherFlagValues.TxSendTimeout,
			TxNotInMempoolTimeout:     txmgr.DefaultBatcherFlagValues.TxNotInMempoolTimeout,
		},
		PrivateTxmgrConfigs: &txmgr.CLIConfig{
			L1RPCURL:                  os.Getenv("L1_WS"),
			NumConfirmations:          0,
			SafeAbortNonceTooLowCount: txmgr.DefaultBatcherFlagValues.SafeAbortNonceTooLowCount,
			PrivateKey:                common.Bytes2Hex(crypto.FromECDSA(l1ProposerPrivKey)),
			FeeLimitMultiplier:        txmgr.DefaultBatcherFlagValues.FeeLimitMultiplier,
			FeeLimitThresholdGwei:     txmgr.DefaultBatcherFlagValues.FeeLimitThresholdGwei,
			MinBaseFeeGwei:            txmgr.DefaultBatcherFlagValues.MinBaseFeeGwei,
			MinTipCapGwei:             txmgr.DefaultBatcherFlagValues.MinTipCapGwei,
			ResubmissionTimeout:       txmgr.DefaultBatcherFlagValues.ResubmissionTimeout,
			ReceiptQueryInterval:      1 * time.Second,
			NetworkTimeout:            txmgr.DefaultBatcherFlagValues.NetworkTimeout,
			TxSendTimeout:             txmgr.DefaultBatcherFlagValues.TxSendTimeout,
			TxNotInMempoolTimeout:     txmgr.DefaultBatcherFlagValues.TxNotInMempoolTimeout,
		},
	}, nil, nil))

	s.p = p
	log.Info("Proposer initialized successfully.")
}

func (s *ProposerTestSuite) emptyMempool() {
	for {
		poolContent, err := s.RPCClient.GetPoolContent(
			context.Background(),
			s.p.proposerAddress,
			s.p.protocolConfigs.BlockMaxGasLimit,
			rpc.BlockMaxTxListBytes,
			s.p.LocalAddresses,
			10,
			0,
			s.p.chainConfig,
		)
		s.Nil(err)

		if len(poolContent) > 0 && len(poolContent[0].TxList) > 0 {
			log.Info("Emptying mempool. Tx count", "txCount", len(poolContent[0].TxList))
			s.Nil(s.p.ProposeOp(context.Background()))
			s.Nil(s.s.ProcessL1Blocks(context.Background()))
			continue
		}
		break
	}
}

func (s *ProposerTestSuite) insertBogusTransactions(
	numberOfTransactionsForEachPrivateKey int,
	numberOfZeroTipTransactions int,
	numberOfWallets int,
) {
	// Empty the mempool before inserting bogus transactions
	s.emptyMempool()

	privetKeyHexList := []string{
		"0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d", // 0x70997970C51812dc3A010C7d01b50e0d17dc79C8
		"0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a", // 0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
		"0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6", // 0x90F79bf6EB2c4f870365E785982E1f101E93b906
		"0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a", // 0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65
		"0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba", // 0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc
	}

	if numberOfWallets > len(privetKeyHexList) {
		s.T().Fatalf("numberOfWallets (%d) is greater than the available private keys (%d)",
			numberOfWallets, len(privetKeyHexList))
	}

	var privateKeys []*ecdsa.PrivateKey

	for i := 0; i < numberOfWallets; i++ {
		priv, err := crypto.ToECDSA(common.FromHex(privetKeyHexList[i]))
		s.Nil(err)
		privateKeys = append(privateKeys, priv)
	}

	// Create a new random source
	randomSource := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, priv := range privateKeys {
		transactOpts, err := bind.NewKeyedTransactorWithChainID(priv, s.RPCClient.L2.ChainID)
		s.Nil(err)
		nonce, err := s.RPCClient.L2.PendingNonceAt(context.Background(), transactOpts.From)
		s.Nil(err)
		// Send bogus transactions to mempool for each private key
		for i := 0; i < numberOfTransactionsForEachPrivateKey; i++ {
			_, err = testutils.AssembleTestTx(s.RPCClient.L2, priv,
				nonce+uint64(i), &transactOpts.From, common.Big1, nil,
				big.NewInt(int64(randomSource.Intn(10)+1)*params.GWei),
				2_100_000,
			)
			s.Nil(err)
		}

		// Add zero tip transactions to the mempool
		for k := 0; k < numberOfZeroTipTransactions; k++ {
			_, err = testutils.AssembleTestTx(s.RPCClient.L2, priv,
				nonce+uint64(numberOfTransactionsForEachPrivateKey+k),
				&transactOpts.From, common.Big1, nil,
				common.Big0,
				2_100_000+uint64(randomSource.Intn(1000)), // Adding a random value between 0 and 999 to 2.1 million
			)
			s.Nil(err)
		}

	}
}

func (s *ProposerTestSuite) TestTxPoolContentWithMinTip() {
	if os.Getenv("L2_NODE") == "l2_reth" {
		s.T().Skip()
	}

	//First input is the number of transactions for each private key,
	//second input is the number of wallets
	numberOfTransactionsForEachPrivateKey := 0
	numberOfPrivateKeys := 5
	numberOfZeroTipTransactions := 300

	s.insertBogusTransactions(numberOfTransactionsForEachPrivateKey, numberOfZeroTipTransactions, numberOfPrivateKeys)

	s.Nil(s.p.ProposeOp(context.Background()))
	s.Nil(s.s.ProcessL1Blocks(context.Background()))
}

func (s *ProposerTestSuite) TestGetRpcPoolContent() {
	//Here we test the getPoolContent function of the proposer
	//We insert a certain number of bogus transactions into the pool
	//and then we test the getPoolContent function with different blockMaxGasLimit and blockMaxTxListBytes
	//and different maxTransactionsLists
	//and different txLengthList
	numberOfTransactionsForEachPrivateKey := 200
	numberOfPrivateKeys := 5
	numberOfZeroTipTransactions := 100
	totalTransactions := (numberOfTransactionsForEachPrivateKey + numberOfZeroTipTransactions) * numberOfPrivateKeys

	s.insertBogusTransactions(numberOfTransactionsForEachPrivateKey, numberOfZeroTipTransactions, numberOfPrivateKeys)

	for _, testCase := range []struct {
		blockMaxGasLimit     uint32
		blockMaxTxListBytes  uint64
		maxTransactionsLists uint64
		txLengthList         []int
		minTip               uint64
	}{
		{
			s.p.protocolConfigs.BlockMaxGasLimit,
			rpc.BlockMaxTxListBytes,
			s.p.MaxProposedTxListsPerEpoch,
			[]int{totalTransactions},
			0,
		},
		{
			s.p.protocolConfigs.BlockMaxGasLimit,
			rpc.BlockMaxTxListBytes,
			s.p.MaxProposedTxListsPerEpoch * 5,
			[]int{totalTransactions},
			0,
		},
		{
			s.p.protocolConfigs.BlockMaxGasLimit / 50,
			rpc.BlockMaxTxListBytes,
			200,
			[]int{129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 81}, //This adds up to 1500
			0,
		},
	} {
		poolContent, err := s.RPCClient.GetPoolContent(
			context.Background(),
			s.p.proposerAddress,
			testCase.blockMaxGasLimit,
			testCase.blockMaxTxListBytes,
			s.p.LocalAddresses,
			testCase.maxTransactionsLists,
			testCase.minTip,
			s.p.chainConfig,
		)
		s.Nil(err)

		s.GreaterOrEqual(int(testCase.maxTransactionsLists), len(poolContent)) //This is to check how many txLists are in poolContent
		for i, txsLen := range testCase.txLengthList {
			s.Equal(txsLen, poolContent[i].TxList.Len())
			s.GreaterOrEqual(uint64(testCase.blockMaxGasLimit), poolContent[i].EstimatedGasUsed)
			s.GreaterOrEqual(testCase.blockMaxTxListBytes, poolContent[i].BytesLength)
		}
	}
}

func (s *ProposerTestSuite) TestProposeOpNoEmptyBlock() {
	// TODO: Temporarily skip this test case when using l2_reth node.
	if os.Getenv("L2_NODE") == "l2_reth" {
		s.T().Skip()
	}
	defer s.Nil(s.s.ProcessL1Blocks(context.Background()))

	p := s.p

	batchSize := 100

	var err error
	for i := 0; i < batchSize; i++ {
		to := common.BytesToAddress(testutils.RandomBytes(32))
		_, err = testutils.SendDynamicFeeTx(s.RPCClient.L2, s.TestAddrPrivKey, &to, nil, nil)
		s.Nil(err)
	}

	var preBuiltTxList []*miner.PreBuiltTxList
	for i := 0; i < 3 && len(preBuiltTxList) == 0; i++ {
		preBuiltTxList, err = s.RPCClient.GetPoolContent(
			context.Background(),
			p.proposerAddress,
			p.protocolConfigs.BlockMaxGasLimit,
			rpc.BlockMaxTxListBytes,
			p.LocalAddresses,
			p.MaxProposedTxListsPerEpoch,
			0,
			p.chainConfig,
		)
		time.Sleep(time.Second)
	}
	s.Nil(err)
	s.Equal(true, len(preBuiltTxList) > 0)

	var (
		blockMinGasLimit    uint64 = math.MaxUint64
		blockMinTxListBytes uint64 = math.MaxUint64
	)
	for _, txs := range preBuiltTxList {
		if txs.EstimatedGasUsed <= blockMinGasLimit {
			blockMinGasLimit = txs.EstimatedGasUsed
		} else {
			break
		}
		if txs.BytesLength <= blockMinTxListBytes {
			blockMinTxListBytes = txs.BytesLength
		} else {
			break
		}
	}

	// Start proposer
	p.LocalAddressesOnly = false
	p.MinGasUsed = blockMinGasLimit
	p.MinTxListBytes = blockMinTxListBytes
	p.MinProposingInternal = time.Minute
	s.Nil(p.ProposeOp(context.Background()))
}

func (s *ProposerTestSuite) TestName() {
	s.Equal("proposer", s.p.Name())
}

func (s *ProposerTestSuite) TestProposeOp() {
	// Propose txs in L2 execution engine's mempool
	sink := make(chan *bindings.TaikoL1ClientBlockProposedV2)
	sub, err := s.p.rpc.TaikoL1.WatchBlockProposedV2(nil, sink, nil)
	s.Nil(err)
	defer func() {
		sub.Unsubscribe()
		close(sink)
	}()

	to := common.BytesToAddress(testutils.RandomBytes(32))
	_, err = testutils.SendDynamicFeeTx(s.p.rpc.L2, s.TestAddrPrivKey, &to, common.Big1, nil)
	s.Nil(err)

	s.Nil(s.p.ProposeOp(context.Background()))

	var (
		event = <-sink
		meta  = metadata.NewTaikoDataBlockMetadataOntake(event)
	)
	s.Equal(meta.GetCoinbase(), s.p.L2SuggestedFeeRecipient)

	_, isPending, err := s.p.rpc.L1.TransactionByHash(context.Background(), meta.GetTxHash())
	s.Nil(err)
	s.False(isPending)

	receipt, err := s.p.rpc.L1.TransactionReceipt(context.Background(), meta.GetTxHash())
	s.Nil(err)
	s.Equal(types.ReceiptStatusSuccessful, receipt.Status)
}

func (s *ProposerTestSuite) TestProposeEmptyBlockOp() {
	s.p.MinProposingInternal = 1 * time.Second
	s.p.lastProposedAt = time.Now().Add(-10 * time.Second)
	s.Nil(s.p.ProposeOp(context.Background()))
}

func (s *ProposerTestSuite) TestProposeTxListOntake() {
	for i := 0; i < int(s.p.protocolConfigs.OntakeForkHeight); i++ {
		s.ProposeAndInsertValidBlock(s.p, s.s)
	}

	l2Head, err := s.p.rpc.L2.HeaderByNumber(context.Background(), nil)
	s.Nil(err)
	s.GreaterOrEqual(l2Head.Number.Uint64(), s.p.protocolConfigs.OntakeForkHeight)

	sink := make(chan *bindings.TaikoL1ClientBlockProposedV2)
	sub, err := s.p.rpc.TaikoL1.WatchBlockProposedV2(nil, sink, nil)
	s.Nil(err)
	defer func() {
		sub.Unsubscribe()
		close(sink)
	}()
	s.Nil(s.p.ProposeTxListOntake(context.Background(), []types.Transactions{{}, {}}))
	s.Nil(s.s.ProcessL1Blocks(context.Background()))

	var l1Height *big.Int
	for i := 0; i < 2; i++ {
		event := <-sink
		if l1Height == nil {
			l1Height = new(big.Int).SetUint64(event.Raw.BlockNumber)
			continue
		}
		s.Equal(l1Height.Uint64(), event.Raw.BlockNumber)
	}

	newL2head, err := s.p.rpc.L2.HeaderByNumber(context.Background(), nil)
	s.Nil(err)

	s.Equal(l2Head.Number.Uint64()+2, newL2head.Number.Uint64())
}

func (s *ProposerTestSuite) TestCalculateInitialWaitTime() {
	// Generate a random offset within the slot time
	timeLapsedSinceLastSlot := float64(rand.Intn(int(SlotTime*1000))) / 1000.0 // Random milliseconds within the slot
	timeLeft := SlotTime - timeLapsedSinceLastSlot
	log.Info("Time left to the next slot", "time", timeLeft)

	// Calculate expected wait time
	expectedWaitTime := timeLeft - TimeGapToPropose
	if expectedWaitTime < 0 {
		expectedWaitTime += SlotTime
	}
	log.Info("Expected wait time:", "expectedWaitTime", expectedWaitTime)

	// Calculate the initial wait time using the existing Proposer instance
	actualWaitTime := s.p.calculateInitialWaitTime(timeLeft)
	log.Info("Actual wait time:", "actualWaitTime", actualWaitTime.Seconds())

	// Assert that the calculated wait time is as expected
	s.InDelta(expectedWaitTime, actualWaitTime.Seconds(), 0.001, "The initial wait time should align with T - 2 seconds")

	log.Info("Finished TestCalculateInitialWaitTime")
}

func (s *ProposerTestSuite) TestProposerStartAndPropose() {
	// Define a context with timeout to avoid indefinite waiting
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(4)*time.Duration(SlotTime)*time.Second+5*time.Second)
	defer cancel()

	// Slice to store proposal timestamps
	proposalTimes := make([]time.Time, 0, 2)

	// Start the Proposer
	currentProposedAt := s.p.lastProposedAt
	log.Info("Starting the Proposer", "lastProposedAt", currentProposedAt)
	err := s.p.Start()
	s.Nil(err, "Failed to start the Proposer")

	// Channel to signal goroutine to exit
	done := make(chan struct{})

	// Goroutine to monitor proposal times
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if currentProposedAt != s.p.lastProposedAt {
					// Record the time when lastProposedAt changed
					proposalTimes = append(proposalTimes, s.p.lastProposedAt)
					currentProposedAt = s.p.lastProposedAt
				}
				if len(proposalTimes) >= 2 {
					close(done) // Signal that we are done
					return
				}
				time.Sleep(500 * time.Millisecond) // Polling interval
			}
		}
	}()

	// Wait for the goroutine to signal completion
	<-done

	// Clean up by canceling the context and closing the proposer
	log.Info("Cleaning up the Proposer")
	s.cancel()
	s.p.Close(context.Background())

	assert.Equal(s.T(), 2, len(proposalTimes), "Should have done two proposals")
	assert.Less(s.T(),
		proposalTimes[1].Sub(proposalTimes[0]),
		time.Duration(SlotTime*float64(time.Second))+100*time.Millisecond,
		"The two proposal should be divided by SlotTime")
}

func (s *ProposerTestSuite) TestProposeOpDoesNotReproposeTxs() {
	// Insert a single bogus transaction
	numberOfTransactionsForEachPrivateKey := 1
	numberOfPrivateKeys := 1
	numberOfZeroTipTransactions := 0
	s.insertBogusTransactions(numberOfTransactionsForEachPrivateKey,
		numberOfZeroTipTransactions, numberOfPrivateKeys)

	// Retrieve the proposed transaction from the pool
	poolContent, err := s.RPCClient.GetPoolContent(
		context.Background(),
		s.p.proposerAddress,
		s.p.protocolConfigs.BlockMaxGasLimit,
		rpc.BlockMaxTxListBytes,
		s.p.LocalAddresses,
		s.p.MaxProposedTxListsPerEpoch,
		0,
		s.p.chainConfig,
	)
	s.Nil(err, "Failed to get pool content")
	s.GreaterOrEqual(len(poolContent), 1, "Pool content should have at least one transaction list")
	s.GreaterOrEqual(poolContent[0].TxList.Len(), 1, "Transaction list should have at least one transaction")

	// Get the hash of the first transaction
	txHash := poolContent[0].TxList[0].Hash()
	log.Info("Proposing transaction", "txHash", txHash)
	// Run ProposeOp to propose the transaction
	s.Nil(s.p.ProposeOp(context.Background()), "ProposeOp should succeed")
	s.Nil(s.s.ProcessL1Blocks(context.Background()))

	// Ensure the transaction is marked as proposed
	hasProposed := s.p.hasBeenProposed(txHash)
	s.True(hasProposed, "Transaction should be marked as proposed")

	// Run ProposeOp again
	log.Info("Running ProposeOp again")
	s.Nil(s.p.ProposeOp(context.Background()))
	s.Nil(s.s.ProcessL1Blocks(context.Background()))

	// Retrieve the pool content again
	log.Info("Retrieving pool content again")
	poolContentAfter, err := s.RPCClient.GetPoolContent(
		context.Background(),
		s.p.proposerAddress,
		s.p.protocolConfigs.BlockMaxGasLimit,
		rpc.BlockMaxTxListBytes,
		s.p.LocalAddresses,
		s.p.MaxProposedTxListsPerEpoch,
		0,
		s.p.chainConfig,
	)
	s.Nil(err, "Failed to get pool content after second ProposeOp")

	// Ensure that the previously proposed transaction is no longer in the pool
	for _, txs := range poolContentAfter {
		for _, tx := range txs.TxList {
			if tx.Hash() == txHash {
				s.Fail("Previously proposed transaction should not be in the pool")
			}
		}
	}
}

func (s *ProposerTestSuite) TestProposalMetrics() {
	// Insert some transactions into the mempool to ensure we have data to propose
	numberOfTransactionsForEachPrivateKey := 5
	numberOfPrivateKeys := 2
	numberOfZeroTipTransactions := 0
	s.insertBogusTransactions(numberOfTransactionsForEachPrivateKey, numberOfZeroTipTransactions, numberOfPrivateKeys)

	// Call ProposeOp to simulate a proposal operation
	err := s.p.ProposeOp(context.Background())
	s.Nil(err, "ProposeOp should succeed")
	s.Nil(s.s.ProcessL1Blocks(context.Background()))

	// Check that the proposal data was stored in the map
	s.p.proposalMutex.Lock()
	defer s.p.proposalMutex.Unlock()

	// Verify there's at least one entry in the proposalData map
	s.GreaterOrEqual(len(s.p.proposalData), 1, "Proposal data should be stored")

	// Get the latest L1 block number - the one that should have been used
	var latestBlockNum uint64
	for blockNum := range s.p.proposalData {
		if blockNum > latestBlockNum {
			latestBlockNum = blockNum
		}
	}

	// Verify the proposal data for this block
	proposalData := s.p.proposalData[latestBlockNum]

	// Verify all the expected fields are present and have reasonable values
	s.NotNil(proposalData.L1Cost, "L1Cost should not be nil")
	s.NotNil(proposalData.TotalEarnings, "TotalEarnings should not be nil")
	s.GreaterOrEqual(proposalData.TotalNumberOfTxs, 0, "TotalNumberOfTxs should be non-negative")
	s.WithinDuration(proposalData.LastProposedAt, time.Now(), 10*time.Second, "LastProposedAt should be recent")

	// Log the metrics for debugging
	log.Info("Proposal metrics recorded",
		"L1BlockNum", latestBlockNum,
		"L1Cost", proposalData.L1Cost.String(),
		"TotalEarnings", proposalData.TotalEarnings.String(),
		"TotalNumberOfTxs", proposalData.TotalNumberOfTxs,
		"LastProposedAt", proposalData.LastProposedAt)

	// Check for metrics update
	// Note: We can't directly check the metrics registry in this test,
	// but we can verify that the data passed to the metrics engine is correct
	// This assumes UpdateBlockMetrics is correctly implemented to use this data
	expectedL1BlockNum := int64(latestBlockNum)
	expectedL1Cost := float64(proposalData.L1Cost.Int64())
	expectedEarnings := float64(proposalData.TotalEarnings.Int64())
	expectedTxCount := int64(proposalData.TotalNumberOfTxs)

	// These verifications ensure that the data that would be recorded by metrics.UpdateBlockMetrics
	// is valid and properly calculated
	s.GreaterOrEqual(expectedL1BlockNum, int64(0), "L1 block number should be valid")
	s.GreaterOrEqual(expectedL1Cost, float64(0), "L1 cost should be non-negative")
	s.GreaterOrEqual(expectedEarnings, float64(0), "Earnings should be non-negative")
	s.GreaterOrEqual(expectedTxCount, int64(0), "Transaction count should be non-negative")
}

func (s *ProposerTestSuite) TearDownTest() {
	s.cancel()
	s.p.Close(context.Background())
}

func TestProposerTestSuite(t *testing.T) {
	log.Error("TestProposerTestSuite started")
	suite.Run(t, new(ProposerTestSuite))
}
