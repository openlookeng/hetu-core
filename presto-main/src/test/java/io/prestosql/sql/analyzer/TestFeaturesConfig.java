/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.operator.aggregation.arrayagg.ArrayAggGroupImplementation;
import io.prestosql.operator.aggregation.histogram.HistogramGroupImplementation;
import io.prestosql.operator.aggregation.multimapagg.MultimapAggGroupImplementation;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType.BLOOM_FILTER;
import static io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType.HASHSET;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.prestosql.sql.analyzer.FeaturesConfig.RedistributeWritesType;
import static io.prestosql.sql.analyzer.FeaturesConfig.RedistributeWritesType.RANDOM;
import static io.prestosql.sql.analyzer.FeaturesConfig.SPILLER_SPILL_PATH;
import static io.prestosql.sql.analyzer.FeaturesConfig.SPILLER_SPILL_PROFILE;
import static io.prestosql.sql.analyzer.FeaturesConfig.SPILLER_SPILL_TO_HDFS;
import static io.prestosql.sql.analyzer.FeaturesConfig.SPILL_ENABLED;
import static io.prestosql.sql.analyzer.RegexLibrary.JONI;
import static io.prestosql.sql.analyzer.RegexLibrary.RE2J;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeaturesConfig.class)
                .setCpuCostWeight(75)
                .setSpillToHdfs(false)
                .setSpillProfile(null)
                .setMemoryCostWeight(10)
                .setNetworkCostWeight(15)
                .setDistributedIndexJoinsEnabled(false)
                .setJoinDistributionType(JoinDistributionType.AUTOMATIC)
                .setJoinMaxBroadcastTableSize(new DataSize(100, MEGABYTE))
                .setGroupedExecutionEnabled(false)
                .setDynamicScheduleForGroupedExecutionEnabled(false)
                .setConcurrentLifespansPerTask(0)
                .setFastInequalityJoins(true)
                .setColocatedJoinsEnabled(false)
                .setSpatialJoinsEnabled(true)
                .setJoinReorderingStrategy(JoinReorderingStrategy.AUTOMATIC)
                .setMaxReorderedJoins(9)
                .setRedistributeWrites(true)
                // redistribute writes type config
                .setRedistributeWritesType(RANDOM)
                .setScaleWriters(false)
                .setWriterMinSize(new DataSize(32, MEGABYTE))
                .setNonEstimatablePredicateApproximationEnabled(true)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setPushTableWriteThroughUnion(true)
                .setDictionaryAggregation(false)
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setSpillEnabled(false)
                .setNonBlockingSpill(false)
                .setSpillBuildForOuterJoinEnabled(false)
                .setInnerJoinSpillFilterEnabled(false)
                .setSpillOrderBy(true)
                .setSpillWindowOperator(true)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("4MB"))
                .setSpillerSpillPaths("")
                .setSpillerThreads(4)
                .setSpillMaxUsedSpaceThreshold(0.9)
                .setMemoryRevokingThreshold(0.9)
                .setMemoryRevokingTarget(0.5)
                .setOptimizeMixedDistinctAggregations(false)
                .setUnwrapCasts(true)
                .setIterativeOptimizerEnabled(true)
                .setIterativeOptimizerTimeout(new Duration(3, MINUTES))
                .setEnableStatsCalculator(true)
                .setIgnoreStatsCalculatorFailures(true)
                .setDefaultFilterFactorEnabled(false)
                .setEnableForcedExchangeBelowGroupId(true)
                .setExchangeCompressionEnabled(false)
                .setEnableIntermediateAggregations(false)
                .setPushAggregationThroughJoin(true)
                .setParseDecimalLiteralsAsDouble(false)
                .setForceSingleNodeOutput(true)
                .setPagesIndexEagerCompactionEnabled(false)
                .setFilterAndProjectMinOutputPageSize(new DataSize(500, KILOBYTE))
                .setFilterAndProjectMinOutputPageRowCount(256)
                .setUseMarkDistinct(true)
                .setPreferPartialAggregation(true)
                .setOptimizeTopNRankingNumber(true)
                .setHistogramGroupImplementation(HistogramGroupImplementation.NEW)
                .setArrayAggGroupImplementation(ArrayAggGroupImplementation.NEW)
                .setMultimapAggGroupImplementation(MultimapAggGroupImplementation.NEW)
                .setDistributedSortEnabled(true)
                .setMaxGroupingSets(2048)
                .setWorkProcessorPipelines(false)
                .setSkipRedundantSort(true)
                .setPredicatePushdownUseTableProperties(true)
                .setEnableDynamicFiltering(true)
                .setDynamicFilteringMaxPerDriverRowCount(10000)
                .setDynamicFilteringDataType(BLOOM_FILTER)
                .setDynamicFilteringWaitTime(new Duration(1000, MILLISECONDS))
                .setDynamicFilteringMaxSize(1000000)
                .setDynamicFilteringMaxPerDriverSize(new DataSize(1, MEGABYTE))
                .setDynamicFilteringBloomFilterFpp(0.1)
                .setQueryPushDown(true)
                .setPushLimitDown(true)
                .setPushLimitThroughOuterJoin(true)
                .setPushLimitThroughSemiJoin(true)
                .setPushLimitThroughUnion(true)
                .setEnableExecutionPlanCache(true)
                .setImplicitConversionEnabled(false)
                .setPushTableThroughSubquery(false)
                .setRewriteFilteringSemiJoinToInnerJoin(false)
                .setTransformSelfJoinToGroupby(true)
                .setSpillReuseExchange(false)
                .setSpillOperatorThresholdReuseExchange(10)
                .setReuseTableScanEnabled(false)
                .setCteReuseEnabled(false)
                .setMaxQueueSize(1024)
                .setMaxPrefetchQueueSize(512)
                .setReuseTableScanEnabled(false)
                .setEnableStarTreeIndex(false)
                .setCubeMetadataCacheSize(5)
                .setCubeMetadataCacheTtl(new Duration(1, HOURS))
                .setImplicitConversionEnabled(false)
                .setLegacyCharToVarcharCoercion(false)
                .setLegacyDateTimestampToVarcharCoercion(false)
                .setLegacyMapSubscript(false)
                .setListBuiltInFunctionsOnly(true)
                .setSortBasedAggregationEnabled(false)
                .setPrcntDriversForPartialAggr(5)
                .setSkipAttachingStatsWithPlan(true)
                .setSkipNonApplicableRulesEnabled(false)
                .setPrioritizeLargerSpiltsMemoryRevoke(true)
                .setRevocableMemorySelectionThreshold(new DataSize(512, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cpu-cost-weight", "0.4")
                .put("memory-cost-weight", "0.3")
                .put("network-cost-weight", "0.2")
                .put("experimental.iterative-optimizer-enabled", "false")
                .put("experimental.iterative-optimizer-timeout", "10s")
                .put("experimental.enable-stats-calculator", "false")
                .put("optimizer.ignore-stats-calculator-failures", "false")
                .put("optimizer.default-filter-factor-enabled", "true")
                .put("enable-forced-exchange-below-group-id", "false")
                .put("distributed-index-joins-enabled", "true")
                .put("optimizer.non-estimatable-predicate-approximation.enabled", "false")
                .put("join-distribution-type", "BROADCAST")
                .put("join-max-broadcast-table-size", "42GB")
                .put("grouped-execution-enabled", "true")
                .put("dynamic-schedule-for-grouped-execution", "true")
                .put("concurrent-lifespans-per-task", "1")
                .put("fast-inequality-joins", "false")
                .put("colocated-joins-enabled", "true")
                .put("spatial-joins-enabled", "false")
                .put("optimizer.join-reordering-strategy", "NONE")
                .put("optimizer.max-reordered-joins", "5")
                .put("redistribute-writes", "false")
                // redistribute writes type config
                .put("redistribute-writes-type", "PARTITIONED")
                .put("scale-writers", "true")
                .put("writer-min-size", "42GB")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.unwrap-casts", "false")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.push-aggregation-through-join", "false")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("experimental.spill-enabled", "true")
                .put("experimental.spill-non-blocking-orderby", "true")
                .put("experimental.spill-build-for-outer-join-enabled", "true")
                .put("experimental.inner-join-spill-filter-enabled", "true")
                .put("experimental.spill-order-by", "false")
                .put("experimental.spill-window-operator", "false")
                .put("experimental.aggregation-operator-unspill-memory-limit", "100MB")
                .put("experimental.spiller-spill-path", "/tmp/hetu/snapshot")
                .put("experimental.spiller-spill-profile", "hdfs")
                .put("experimental.spiller-spill-to-hdfs", "true")
                .put("experimental.spiller-threads", "42")
                .put("experimental.spiller-max-used-space-threshold", "0.8")
                .put("experimental.memory-revoking-threshold", "0.2")
                .put("experimental.memory-revoking-target", "0.8")
                .put("exchange.compression-enabled", "true")
                .put("optimizer.enable-intermediate-aggregations", "true")
                .put("parse-decimal-literals-as-double", "true")
                .put("optimizer.force-single-node-output", "false")
                .put("pages-index.eager-compaction-enabled", "true")
                .put("experimental.filter-and-project-min-output-page-size", "1MB")
                .put("experimental.filter-and-project-min-output-page-row-count", "2048")
                .put("histogram.implementation", "LEGACY")
                .put("arrayagg.implementation", "LEGACY")
                .put("multimapagg.implementation", "LEGACY")
                .put("optimizer.use-mark-distinct", "false")
                .put("optimizer.prefer-partial-aggregation", "false")
                .put("optimizer.optimize-top-n-ranking-number", "false")
                .put("distributed-sort", "false")
                .put("analyzer.max-grouping-sets", "2047")
                .put("experimental.work-processor-pipelines", "true")
                .put("optimizer.skip-redundant-sort", "false")
                .put("optimizer.predicate-pushdown-use-table-properties", "false")
                .put("enable-dynamic-filtering", "false")
                .put("experimental.enable-execution-plan-cache", "false")
                .put("hetu.query-pushdown", "false")
                .put("optimizer.push-limit-down", "false")
                .put("optimizer.push-limit-through-union", "false")
                .put("optimizer.push-limit-through-semi-join", "false")
                .put("optimizer.push-limit-through-outer-join", "false")
                .put("dynamic-filtering-wait-time", "200ms")
                .put("dynamic-filtering-max-size", "10000")
                .put("dynamic-filtering-max-per-driver-row-count", "256")
                .put("dynamic-filtering-data-type", "HASHSET")
                .put("dynamic-filtering-max-per-driver-size", "64kB")
                .put("dynamic-filtering-bloom-filter-fpp", "0.001")
                .put("implicit-conversion", "true")
                .put("optimizer.push-table-through-subquery", "true")
                .put("optimizer.rewrite-filtering-semi-join-to-inner-join", "true")
                .put("optimizer.transform-self-join-to-groupby", "false")
                .put("optimizer.reuse-table-scan", "true")
                .put("experimental.spill-reuse-tablescan", "true")
                .put("experimental.spill-threshold-reuse-tablescan", "100")
                .put("optimizer.cte-reuse-enabled", "true")
                .put("cte.cte-max-queue-size", "2048")
                .put("cte.cte-max-prefetch-queue-size", "1024")
                .put("cube.metadata-cache-size", "10")
                .put("cube.metadata-cache-ttl", "10m")
                .put("optimizer.enable-star-tree-index", "true")
                .put("deprecated.legacy-char-to-varchar-coercion", "true")
                .put("deprecated.legacy-date-timestamp-to-varchar-coercion", "true")
                .put("deprecated.legacy-map-subscript", "true")
                .put("list-built-in-functions-only", "false")
                .put("optimizer.sort-based-aggregation-enabled", "true")
                .put("sort.prcnt-drivers-for-partial-aggr", "55")
                .put("optimizer.skip-attaching-stats-with-plan", "false")
                .put("optimizer.skip-non-applicable-rules-enabled", "true")
                .put("experimental.prioritize-larger-spilts-memory-revoke", "false")
                .put("experimental.revocable-memory-selection-threshold", "500MB")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setCpuCostWeight(0.4)
                .setMemoryCostWeight(0.3)
                .setNetworkCostWeight(0.2)
                .setIterativeOptimizerEnabled(false)
                .setIterativeOptimizerTimeout(new Duration(10, SECONDS))
                .setEnableStatsCalculator(false)
                .setIgnoreStatsCalculatorFailures(false)
                .setEnableForcedExchangeBelowGroupId(false)
                .setDistributedIndexJoinsEnabled(true)
                .setJoinDistributionType(BROADCAST)
                .setJoinMaxBroadcastTableSize(new DataSize(42, GIGABYTE))
                .setGroupedExecutionEnabled(true)
                .setDynamicScheduleForGroupedExecutionEnabled(true)
                .setConcurrentLifespansPerTask(1)
                .setFastInequalityJoins(false)
                .setColocatedJoinsEnabled(true)
                .setSpatialJoinsEnabled(false)
                .setJoinReorderingStrategy(NONE)
                .setMaxReorderedJoins(5)
                .setRedistributeWrites(false)
                // redistribute writes type config
                .setRedistributeWritesType(RedistributeWritesType.PARTITIONED)
                .setScaleWriters(true)
                .setWriterMinSize(new DataSize(42, GIGABYTE))
                .setNonEstimatablePredicateApproximationEnabled(false)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeMixedDistinctAggregations(true)
                .setUnwrapCasts(false)
                .setPushTableWriteThroughUnion(false)
                .setPushTableThroughSubquery(true)
                .setDictionaryAggregation(true)
                .setPushAggregationThroughJoin(false)
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setSpillEnabled(true)
                .setNonBlockingSpill(true)
                .setSpillBuildForOuterJoinEnabled(true)
                .setInnerJoinSpillFilterEnabled(true)
                .setSpillOrderBy(false)
                .setSpillWindowOperator(false)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("100MB"))
                .setSpillerSpillPaths("/tmp/hetu/snapshot")
                .setSpillToHdfs(true)
                .setSpillProfile("hdfs")
                .setSpillerThreads(42)
                .setSpillMaxUsedSpaceThreshold(0.8)
                .setMemoryRevokingThreshold(0.2)
                .setMemoryRevokingTarget(0.8)
                .setExchangeCompressionEnabled(true)
                .setEnableIntermediateAggregations(true)
                .setParseDecimalLiteralsAsDouble(true)
                .setForceSingleNodeOutput(false)
                .setPagesIndexEagerCompactionEnabled(true)
                .setFilterAndProjectMinOutputPageSize(new DataSize(1, MEGABYTE))
                .setFilterAndProjectMinOutputPageRowCount(2048)
                .setUseMarkDistinct(false)
                .setPreferPartialAggregation(false)
                .setOptimizeTopNRankingNumber(false)
                .setHistogramGroupImplementation(HistogramGroupImplementation.LEGACY)
                .setArrayAggGroupImplementation(ArrayAggGroupImplementation.LEGACY)
                .setMultimapAggGroupImplementation(MultimapAggGroupImplementation.LEGACY)
                .setDistributedSortEnabled(false)
                .setMaxGroupingSets(2047)
                .setDefaultFilterFactorEnabled(true)
                .setWorkProcessorPipelines(true)
                .setSkipRedundantSort(false)
                .setPredicatePushdownUseTableProperties(false)
                .setEnableDynamicFiltering(false)
                .setQueryPushDown(false)
                .setImplicitConversionEnabled(true)
                .setPushLimitDown(false)
                .setPushLimitThroughUnion(false)
                .setPushLimitThroughSemiJoin(false)
                .setPushLimitThroughOuterJoin(false)
                .setRewriteFilteringSemiJoinToInnerJoin(true)
                .setEnableExecutionPlanCache(false)
                .setDynamicFilteringMaxPerDriverRowCount(256)
                .setDynamicFilteringDataType(HASHSET)
                .setDynamicFilteringWaitTime(new Duration(200, MILLISECONDS))
                .setDynamicFilteringMaxSize(10000)
                .setDynamicFilteringMaxPerDriverSize(new DataSize(64, KILOBYTE))
                .setDynamicFilteringBloomFilterFpp(0.001)
                .setTransformSelfJoinToGroupby(false)
                .setReuseTableScanEnabled(true)
                .setSpillReuseExchange(true)
                .setSpillOperatorThresholdReuseExchange(100)
                .setCteReuseEnabled(true)
                .setMaxQueueSize(2048)
                .setMaxPrefetchQueueSize(1024)
                .setSpillOperatorThresholdReuseExchange(100)
                .setEnableStarTreeIndex(true)
                .setCubeMetadataCacheSize(10)
                .setCubeMetadataCacheTtl(new Duration(10, MINUTES))
                .setLegacyCharToVarcharCoercion(true)
                .setLegacyDateTimestampToVarcharCoercion(true)
                .setLegacyMapSubscript(true)
                .setListBuiltInFunctionsOnly(false)
                .setSortBasedAggregationEnabled(true)
                .setPrcntDriversForPartialAggr(55)
                .setSkipAttachingStatsWithPlan(false)
                .setSkipNonApplicableRulesEnabled(true)
                .setPrioritizeLargerSpiltsMemoryRevoke(false)
                .setRevocableMemorySelectionThreshold(new DataSize(500, MEGABYTE));

        assertFullMapping(properties, expected);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*\\Q" + SPILLER_SPILL_PATH + " must be configured when " + SPILL_ENABLED + " is set to true\\E.*")
    public void testValidateSpillConfiguredIfEnabled()
    {
        new ConfigurationFactory(ImmutableMap.of(SPILL_ENABLED, "true"))
                .build(FeaturesConfig.class);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*\\Q" + SPILLER_SPILL_PATH + " must be only contain single path when " + SPILLER_SPILL_TO_HDFS + "\\E.*")
    public void testValidateSpillToHdfsIfWrongPathConfigured()
    {
        new ConfigurationFactory(ImmutableMap.of(SPILL_ENABLED, "true", SPILLER_SPILL_TO_HDFS, "true", SPILLER_SPILL_PROFILE, "spill_profile", SPILLER_SPILL_PATH, "/abc/def,/abc/def/ghi"))
                .build(FeaturesConfig.class);
    }

    @Test
    public void testValidateSpillToHdfsPathIfConfigured()
    {
        new ConfigurationFactory(ImmutableMap.of(SPILL_ENABLED, "true", SPILLER_SPILL_TO_HDFS, "true", SPILLER_SPILL_PROFILE, "spill_profile", SPILLER_SPILL_PATH, "/tmp/hetu/snapshot"))
                .build(FeaturesConfig.class);
    }
}
