package metrics

import (
	"context"
	"math"
	"strconv"

	opMetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	txmgrMetrics "github.com/ethereum-optimism/optimism/op-service/txmgr/metrics"
	"github.com/ethereum/go-ethereum/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"

	"github.com/taikoxyz/taiko-mono/packages/taiko-client/cmd/flags"
	"github.com/taikoxyz/taiko-mono/packages/taiko-client/pkg/rpc"
)

// WeiToEtherDivisor represents the conversion factor from wei to ether (1 ether = 10^18 wei)
const WeiToEtherDivisor = 1e18

// WeiToGweiDivisor represents the conversion factor from wei to gwei (1 gwei = 10^9 wei)
const WeiToGweiDivisor = 1e9

// Metrics
var (
	registry = opMetrics.NewRegistry()
	factory  = opMetrics.With(registry)

	// Driver
	DriverL1HeadHeightGauge     = factory.NewGauge(prometheus.GaugeOpts{Name: "driver_l1Head_height"})
	DriverL2HeadHeightGauge     = factory.NewGauge(prometheus.GaugeOpts{Name: "driver_l2Head_height"})
	DriverL1CurrentHeightGauge  = factory.NewGauge(prometheus.GaugeOpts{Name: "driver_l1Current_height"})
	DriverL2HeadIDGauge         = factory.NewGauge(prometheus.GaugeOpts{Name: "driver_l2Head_id"})
	DriverL2VerifiedHeightGauge = factory.NewGauge(prometheus.GaugeOpts{Name: "driver_l2Verified_id"})

	// Proposer
	ProposerProposeEpochCounter    = factory.NewCounter(prometheus.CounterOpts{Name: "proposer_epoch"})
	ProposerProposedTxListsCounter = factory.NewCounter(prometheus.CounterOpts{Name: "proposer_proposed_txLists"})
	ProposerProposedTxsCounter     = factory.NewCounter(prometheus.CounterOpts{Name: "proposer_proposed_txs"})
	ProposerPoolContentFetchTime   = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_pool_content_fetch_time"})
	ProposerEstimatedCostCalldata  = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_estimated_cost_calldata"})
	ProposerEstimatedCostBlob      = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_estimated_cost_blob"})
	ProposerProposeByCalldata      = factory.NewCounter(prometheus.CounterOpts{Name: "proposer_propose_by_calldata"})
	ProposerProposeByBlob          = factory.NewCounter(prometheus.CounterOpts{Name: "proposer_propose_by_blob"})
	ProposerCostEstimationError    = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_cost_estimation_error"})
	// New metrics for ProposeOp
	// Block-specific metrics with L1 block number as a label
	ProposerL1Cost = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_l1_cost",
		},
	)

	ProposerL1CostByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l1_cost_by_block",
		},
		[]string{"block_number"},
	)

	ProposerEarnings = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_earnings",
		},
	)

	ProposerEarningsByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_earnings_by_block",
		},
		[]string{"block_number"},
	)

	ProposerProposedAt = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_proposed_at",
		},
	)

	ProposerProposedAtByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_proposed_at_by_block",
		},
		[]string{"block_number"},
	)

	ProposerNumberOfTxs = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_number_of_txs",
		},
	)

	ProposerNumberOfTxsByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_number_of_txs_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL1BaseFee = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_l1_base_fee",
		},
	)

	ProposerL1BaseFeeByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l1_base_fee_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL1PriorityFee = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_l1_priority_fee",
		},
	)

	ProposerL1PriorityFeeByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l1_priority_fee_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL2BaseFee = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "proposer_l2_base_fee",
		},
	)

	ProposerL2BaseFeeByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l2_base_fee_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL2BlockNumberByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l2_block_number_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL2ActualNumberOfTxsByL2Block = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l2_actual_number_of_txs_by_l2_block",
		},
		[]string{"l2_block_number"},
	)

	ProposerBlockProposedToTaikoByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_block_proposed_to_taiko_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL1ActualTotalFeeByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l1_actual_total_fee_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL1ActualGasTipCapByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l1_actual_gas_tip_cap_by_block",
		},
		[]string{"block_number"},
	)

	ProposerL1TxAssociatedL2BlockIdByBlock = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proposer_l1_tx_associated_l2_block_id_by_block",
		},
		[]string{"block_number"},
	)

	ProposerTotalDuration            = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_total_duration"})
	ProposerFetchPoolContentDuration = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_fetch_pool_content_duration"})
	ProposerProposeTxListsDuration   = factory.NewGauge(prometheus.GaugeOpts{Name: "proposer_propose_tx_lists_duration"})

	// Prover
	ProverLatestVerifiedIDGauge      = factory.NewGauge(prometheus.GaugeOpts{Name: "prover_latestVerified_id"})
	ProverLatestProvenBlockIDGauge   = factory.NewGauge(prometheus.GaugeOpts{Name: "prover_latestProven_id"})
	ProverQueuedProofCounter         = factory.NewCounter(prometheus.CounterOpts{Name: "prover_proof_all_queued"})
	ProverReceivedProofCounter       = factory.NewCounter(prometheus.CounterOpts{Name: "prover_proof_all_received"})
	ProverSentProofCounter           = factory.NewCounter(prometheus.CounterOpts{Name: "prover_proof_all_sent"})
	ProverProofsAssigned             = factory.NewCounter(prometheus.CounterOpts{Name: "prover_proof_assigned"})
	ProverReceivedProposedBlockGauge = factory.NewGauge(prometheus.GaugeOpts{Name: "prover_proposed_received"})
	ProverReceivedProvenBlockGauge   = factory.NewGauge(prometheus.GaugeOpts{Name: "prover_proven_received"})
	ProverProvenByGuardianGauge      = factory.NewGauge(prometheus.GaugeOpts{Name: "prover_proven_by_guardian"})
	ProverSubmissionAcceptedCounter  = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_submission_accepted",
	})
	ProverSubmissionErrorCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_submission_error",
	})
	ProverAggregationSubmissionErrorCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_aggregation_submission_error",
	})
	ProverSGXAggregationGenerationTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "prover_proof_sgx_aggregation_generation_time",
	})
	ProverSgxProofGeneratedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_sgx_generated",
	})
	ProverSgxProofGenerationTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "prover_proof_sgx_generation_time",
	})
	ProverSgxProofAggregationGeneratedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_sgx_aggregation_generated",
	})
	ProverR0AggregationGenerationTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "prover_proof_r0_aggregation_generation_time",
	})
	ProverR0ProofGeneratedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_r0_generated",
	})
	ProverR0ProofGenerationTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "prover_proof_r0_generation_time",
	})
	ProverR0ProofAggregationGeneratedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_r0_aggregation_generated",
	})
	ProverSP1AggregationGenerationTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "prover_proof_sp1_aggregation_generation_time",
	})
	ProverSp1ProofGeneratedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_sp1_generated",
	})
	ProverSP1ProofGenerationTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "prover_proof_sp1_generation_time",
	})
	ProverSp1ProofAggregationGeneratedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_sp1_aggregation_generated",
	})
	ProverSubmissionRevertedCounter = factory.NewCounter(prometheus.CounterOpts{
		Name: "prover_proof_submission_reverted",
	})

	// TxManager
	TxMgrMetrics = txmgrMetrics.MakeTxMetrics("client", factory)
)

// Serve starts the metrics server on the given address, will be closed when the given
// context is cancelled.
func Serve(ctx context.Context, c *cli.Context) error {
	if !c.Bool(flags.MetricsEnabled.Name) {
		return nil
	}

	log.Info(
		"Starting metrics server",
		"host", c.String(flags.MetricsAddr.Name),
		"port", c.Int(flags.MetricsPort.Name),
	)

	server, err := opMetrics.StartServer(
		registry,
		c.String(flags.MetricsAddr.Name),
		c.Int(flags.MetricsPort.Name),
	)
	if err != nil {
		return err
	}

	defer func() {
		if err := server.Stop(ctx); err != nil {
			log.Error("Failed to close metrics server", "error", err)
		}
	}()

	rpc.BlockOnInterruptsContext(ctx)

	return nil
}

// UpdateBlockMetrics updates the per-block metrics for a specific L1 block
func UpdateBlockMetrics(
	l1BlockNumber int64,
	l1Cost float64,
	totalEarnings float64,
	proposedAt float64,
	totalNumberOfTxs int64,
	L1BaseFee float64,
	L1PriorityFee float64,
	L2BaseFee float64,
	L2BlockNumber int64,
	blockProposedToTaiko bool,
	l1TxtotalFee float64,
	l1TxGasTipCap float64,
	L2NumberOfTxs int64,
	l1TxAssociatedl2BlockId uint64,
) {
	// Convert from wei to ether
	l1CostInEther := l1Cost / WeiToEtherDivisor
	totalEarningsInEther := totalEarnings / WeiToEtherDivisor

	// Convert from wei to gwei
	l1BaseFeeInGwei := L1BaseFee / WeiToGweiDivisor
	l1PriorityFeeInGwei := L1PriorityFee / WeiToGweiDivisor
	l2BaseFeeInGwei := L2BaseFee / WeiToGweiDivisor

	ProposerL1Cost.Set(l1CostInEther)
	ProposerEarnings.Set(totalEarningsInEther)
	ProposerProposedAt.Set(float64(proposedAt))
	ProposerNumberOfTxs.Set(float64(totalNumberOfTxs))
	ProposerL1BaseFee.Set(l1BaseFeeInGwei)
	ProposerL1PriorityFee.Set(l1PriorityFeeInGwei)
	ProposerL2BaseFee.Set(l2BaseFeeInGwei)

	l1BlockNumberStr := strconv.FormatInt(l1BlockNumber, 10)

	ProposerL1CostByBlock.WithLabelValues(l1BlockNumberStr).Set(l1CostInEther)
	ProposerEarningsByBlock.WithLabelValues(l1BlockNumberStr).Set(totalEarningsInEther)
	ProposerProposedAtByBlock.WithLabelValues(l1BlockNumberStr).Set(math.Round(proposedAt*10000) / 10000)
	ProposerNumberOfTxsByBlock.WithLabelValues(l1BlockNumberStr).Set(float64(totalNumberOfTxs))
	ProposerL1BaseFeeByBlock.WithLabelValues(l1BlockNumberStr).Set(l1BaseFeeInGwei)
	ProposerL1PriorityFeeByBlock.WithLabelValues(l1BlockNumberStr).Set(l1PriorityFeeInGwei)

	// Convert boolean to float64: 1.0 for true, 0.0 for false
	boolValue := 0.0
	if blockProposedToTaiko {
		boolValue = 1.0
	}

	l1TxTotalFeeInEther := l1TxtotalFee / WeiToEtherDivisor
	l1TxGasTipCapInGwei := l1TxGasTipCap / WeiToGweiDivisor

	ProposerBlockProposedToTaikoByBlock.WithLabelValues(l1BlockNumberStr).Set(boolValue)
	ProposerL1ActualTotalFeeByBlock.WithLabelValues(l1BlockNumberStr).Set(l1TxTotalFeeInEther)
	ProposerL1ActualGasTipCapByBlock.WithLabelValues(l1BlockNumberStr).Set(l1TxGasTipCapInGwei)
	ProposerL1TxAssociatedL2BlockIdByBlock.WithLabelValues(l1BlockNumberStr).Set(float64(l1TxAssociatedl2BlockId))

	ProposerL2ActualNumberOfTxsByL2Block.WithLabelValues(strconv.FormatInt(L2BlockNumber, 10)).Set(float64(L2NumberOfTxs))

}
