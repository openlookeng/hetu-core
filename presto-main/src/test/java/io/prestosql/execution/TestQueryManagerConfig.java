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
package io.prestosql.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.exchange.RetryPolicy;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.execution.QueryManagerConfig.AVAILABLE_HEAP_MEMORY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestQueryManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(QueryManagerConfig.class)
                .setMinQueryExpireAge(new Duration(15, TimeUnit.MINUTES))
                .setMaxQueryHistory(100)
                .setMaxQueryLength(1_000_000)
                .setMaxStageCount(100)
                .setStageCountWarningThreshold(50)
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setScheduleSplitBatchSize(1000)
                .setMinScheduleSplitBatchSize(100)
                .setMaxConcurrentQueries(1000)
                .setMaxQueuedQueries(5000)
                .setInitialHashPartitions(100)
                .setQueryManagerExecutorPoolSize(5)
                .setRemoteTaskMinErrorDuration(new Duration(5, TimeUnit.MINUTES))
                .setRemoteTaskMaxErrorDuration(new Duration(5, TimeUnit.MINUTES))
                .setRemoteTaskMaxCallbackThreads(1000)
                .setQueryExecutionPolicy("all-at-once")
                .setQueryMaxRunTime(new Duration(100, TimeUnit.DAYS))
                .setQueryMaxExecutionTime(new Duration(100, TimeUnit.DAYS))
                .setQueryMaxCpuTime(new Duration(1_000_000_000, TimeUnit.DAYS))
                .setRequiredWorkers(1)
                .setRequiredWorkersMaxWait(new Duration(5, TimeUnit.MINUTES))
                .setRetryPolicy(RetryPolicy.NONE)
                .setQueryRetryAttempts(4)
                .setTaskRetryAttemptsPerTask(4)
                .setTaskRetryAttemptsOverall(Integer.MAX_VALUE)
                .setRetryInitialDelay(new Duration(10, SECONDS))
                .setRetryMaxDelay(new Duration(1, MINUTES))
                .setRetryDelayScaleFactor(2.0)
                .setMaxTasksWaitingForNodePerStage(5)
                .setFaultTolerantExecutionTargetTaskInputSize(new DataSize(4, GIGABYTE))
                .setFaultTolerantExecutionTargetTaskSplitCount(64)
                .setFaultTolerantPreserveInputPartitionsInWriteStage(true)
                .setFaultTolerantExecutionMinTaskSplitCount(16)
                .setFaultTolerantExecutionMaxTaskSplitCount(256)
                .setFaultTolerantExecutionPartitionCount(50)
                .setFaultTolerantExecutionTaskDescriptorStorageMaxMemory(new DataSize(Math.round(AVAILABLE_HEAP_MEMORY * 0.15), DataSize.Unit.BYTE))
                .setExchangeFilesystemBaseDirectory("/tmp/hetu-exchange-manager")
                .setExchangeFilesystemType("local"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.client.timeout", "10s")
                .put("query.min-expire-age", "30s")
                .put("query.max-history", "10")
                .put("query.max-length", "10000")
                .put("query.max-stage-count", "12345")
                .put("query.stage-count-warning-threshold", "12300")
                .put("query.schedule-split-batch-size", "99")
                .put("query.min-schedule-split-batch-size", "9")
                .put("query.max-concurrent-queries", "10")
                .put("query.max-queued-queries", "15")
                .put("query.initial-hash-partitions", "16")
                .put("query.manager-executor-pool-size", "11")
                .put("query.remote-task.min-error-duration", "30s")
                .put("query.remote-task.max-error-duration", "60s")
                .put("query.remote-task.max-callback-threads", "10")
                .put("query.execution-policy", "phased")
                .put("query.max-run-time", "2h")
                .put("query.max-execution-time", "3h")
                .put("query.max-cpu-time", "2d")
                .put("query-manager.required-workers", "333")
                .put("query-manager.required-workers-max-wait", "33m")
                .put("retry-policy", "TASK")
                .put("query-retry-attempts", "5")
                .put("task-retry-attempts-overall", "20")
                .put("task-retry-attempts-per-task", "5")
                .put("retry-initial-delay", "11s")
                .put("retry-max-delay", "2m")
                .put("retry-delay-scale-factor", "3.0")
                .put("max-tasks-waiting-for-node-per-stage", "6")
                .put("fault-tolerant-execution-target-task-input-size", "5GB")
                .put("fault-tolerant-execution-target-task-split-count", "65")
                .put("fault-tolerant-execution-preserve-input-partitions-in-write-stage", "false")
                .put("fault-tolerant-execution-min-task-split-count", "17")
                .put("fault-tolerant-execution-max-task-split-count", "257")
                .put("fault-tolerant-execution-partition-count", "51")
                .put("fault-tolerant-execution-task-descriptor-storage-max-memory", "1GB")
                .put("exchange-filesystem-base-directory", "/opt/hetu-1.8.0/exchange-base-dir")
                .put("exchange-filesystem-type", "hdfs")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setMinQueryExpireAge(new Duration(30, TimeUnit.SECONDS))
                .setMaxQueryHistory(10)
                .setMaxQueryLength(10000)
                .setMaxStageCount(12345)
                .setStageCountWarningThreshold(12300)
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setScheduleSplitBatchSize(99)
                .setMinScheduleSplitBatchSize(9)
                .setMaxConcurrentQueries(10)
                .setMaxQueuedQueries(15)
                .setInitialHashPartitions(16)
                .setQueryManagerExecutorPoolSize(11)
                .setRemoteTaskMinErrorDuration(new Duration(60, TimeUnit.SECONDS))
                .setRemoteTaskMaxErrorDuration(new Duration(60, TimeUnit.SECONDS))
                .setRemoteTaskMaxCallbackThreads(10)
                .setQueryExecutionPolicy("phased")
                .setQueryMaxRunTime(new Duration(2, TimeUnit.HOURS))
                .setQueryMaxExecutionTime(new Duration(3, TimeUnit.HOURS))
                .setQueryMaxCpuTime(new Duration(2, TimeUnit.DAYS))
                .setRequiredWorkers(333)
                .setRequiredWorkersMaxWait(new Duration(33, TimeUnit.MINUTES))
                .setRetryPolicy(RetryPolicy.TASK)
                .setQueryRetryAttempts(5)
                .setTaskRetryAttemptsPerTask(5)
                .setTaskRetryAttemptsOverall(20)
                .setRetryInitialDelay(new Duration(11, SECONDS))
                .setRetryMaxDelay(new Duration(2, MINUTES))
                .setRetryDelayScaleFactor(3.0)
                .setMaxTasksWaitingForNodePerStage(6)
                .setFaultTolerantExecutionTargetTaskInputSize(new DataSize(5, GIGABYTE))
                .setFaultTolerantExecutionTargetTaskSplitCount(65)
                .setFaultTolerantPreserveInputPartitionsInWriteStage(false)
                .setFaultTolerantExecutionMinTaskSplitCount(17)
                .setFaultTolerantExecutionMaxTaskSplitCount(257)
                .setFaultTolerantExecutionPartitionCount(51)
                .setFaultTolerantExecutionTaskDescriptorStorageMaxMemory(new DataSize(1, DataSize.Unit.GIGABYTE))
                .setExchangeFilesystemBaseDirectory("/opt/hetu-1.8.0/exchange-base-dir")
                .setExchangeFilesystemType("hdfs");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
