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
package io.prestosql.execution.scheduler;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.client.NodeVersion;
import io.prestosql.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.prestosql.memory.MemoryInfo;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.memory.MemoryPoolInfo;
import io.prestosql.testing.TestingSession;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.prestosql.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.prestosql.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExponentialGrowthPartitionMemoryEstimator
{
    @Test
    public void testEstimator()
    {
        InternalNodeManager nodeManager = new InMemoryNodeManager(URI.create("local://blah"));
        BinPackingNodeAllocatorService nodeAllocatorService = new BinPackingNodeAllocatorService(
                nodeManager,
                () -> ImmutableMap.of(new InternalNode("a-node", URI.create("local://blah"), NodeVersion.UNKNOWN, false).getNodeIdentifier(), Optional.of(buildWorkerMemoryInfo(new DataSize(0, BYTE)))),
                false,
                Duration.of(1, MINUTES),
                new DataSize(0, BYTE),
                Ticker.systemTicker());
        nodeAllocatorService.refreshNodePoolMemoryInfos();
        PartitionMemoryEstimator estimator = nodeAllocatorService.createPartitionMemoryEstimator();

        Session session = TestingSession.testSessionBuilder().build();

        assertThat(estimator.getInitialMemoryRequirements(session, new DataSize(107, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(new DataSize(107, MEGABYTE), false));

        // peak memory of failed task 10MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(new DataSize(50, MEGABYTE), false),
                        new DataSize(10, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(new DataSize(50, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(new DataSize(50, MEGABYTE), false),
                        new DataSize(10, MEGABYTE),
                        CLUSTER_OUT_OF_MEMORY.toErrorCode()))
                .isEqualTo(new MemoryRequirements(new DataSize(150, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(new DataSize(50, MEGABYTE), false),
                        new DataSize(10, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(new DataSize(150, MEGABYTE), false));

        // peak memory of failed task 70MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(new DataSize(50, MEGABYTE), false),
                        new DataSize(70, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(new DataSize(70, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(new DataSize(50, MEGABYTE), false),
                        new DataSize(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(new DataSize(210, MEGABYTE), false));

        // register a couple successful attempts; 90th percentile is at 300MB
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(1000, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(100, MEGABYTE), true, Optional.empty());

        // for initial we should pick estimate if greater than default
        assertThat(estimator.getInitialMemoryRequirements(session, new DataSize(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(new DataSize(300, MEGABYTE), false));

        // if default memory requirements is greater than estimate it should be picked still
        assertThat(estimator.getInitialMemoryRequirements(session, new DataSize(500, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(new DataSize(500, MEGABYTE), false));

        // for next we should still pick current initial if greater
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(new DataSize(50, MEGABYTE), false),
                        new DataSize(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(new DataSize(300, MEGABYTE), false));

        // a couple oom errors are registered
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 200*3 (600)
        assertThat(estimator.getInitialMemoryRequirements(session, new DataSize(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(new DataSize(600, MEGABYTE), false));

        // a couple oom errors are registered with requested memory greater than peak
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(300, MEGABYTE), false), new DataSize(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(300, MEGABYTE), false), new DataSize(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(300, MEGABYTE), false), new DataSize(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 300*3 (900)
        assertThat(estimator.getInitialMemoryRequirements(session, new DataSize(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(new DataSize(900, MEGABYTE), false));

        // other errors should not change estimate
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(new DataSize(100, MEGABYTE), false), new DataSize(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));

        assertThat(estimator.getInitialMemoryRequirements(session, new DataSize(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(new DataSize(900, MEGABYTE), false));
    }

    private MemoryInfo buildWorkerMemoryInfo(DataSize usedMemory)
    {
        return new MemoryInfo(
                4,
                0,
                0,
                new DataSize(128, GIGABYTE),
                ImmutableMap.of(new MemoryPoolId("general"), new MemoryPoolInfo(
                        new DataSize(64, GIGABYTE).toBytes(),
                        usedMemory.toBytes(),
                        0,
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of())));
    }
}
