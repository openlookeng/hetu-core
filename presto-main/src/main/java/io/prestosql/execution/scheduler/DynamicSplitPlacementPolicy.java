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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.service.PropertyService;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

public class DynamicSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    //TODO the following variables will be removed after implementing Discovery Service AA mode
    public static final String QUERY_MANAGER_REQUIRED_WORKERS_MAX_WAIT = "query-manager.required-workers-max-wait";
    private static final Logger LOG = Logger.get(DynamicSplitPlacementPolicy.class);
    private final NodeSelector nodeSelector;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;
    private final Duration defaultMaxComputeAssignmentDuration = new Duration(1, TimeUnit.MINUTES);
    private final long computeAssignmentSleepMilliSeconds = 5000;

    public DynamicSplitPlacementPolicy(NodeSelector nodeSelector, Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, SqlStageExecution stage)
    {
        ensureClusterReady();
        return nodeSelector.computeAssignments(splits, remoteTasks.get(), Optional.of(stage));
    }

    private void ensureClusterReady()
    {
        //TODO for-loop will be removed after implementing discovery Service AA mode(now with AP mode)
        /*
            Note: After main coordinator/discovery dies, it generally takes 10-20 seconds to recover, so
                  double the time here
         */
        Duration maxComputeAssignmentRetryDuration = defaultMaxComputeAssignmentDuration;

        try {
            maxComputeAssignmentRetryDuration = PropertyService.getDurationProperty(QUERY_MANAGER_REQUIRED_WORKERS_MAX_WAIT);
        }
        catch (Exception e) {
            LOG.warn(e.getMessage());
        }
        int maxComputeAssignmentRetry = (int) (maxComputeAssignmentRetryDuration.toMillis() / computeAssignmentSleepMilliSeconds);

        int retry = 0;
        while (retry < maxComputeAssignmentRetry) {
            if (!nodeSelector.allNodes().isEmpty()) {
                break;
            }
            LOG.debug("Will not compute assignment since node map is empty, will retry at times: " + (++retry));
            try {
                Thread.sleep(computeAssignmentSleepMilliSeconds);
            }
            catch (InterruptedException e) {
                // could be ignored
            }
        }

        if (retry >= maxComputeAssignmentRetry) {
            throw new PrestoException(NO_NODES_AVAILABLE, "Cluster may not be ready, no nodes available to run query at this moment");
        }
    }

    @Override
    public void lockDownNodes()
    {
        nodeSelector.lockDownNodes();
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return nodeSelector.allNodes();
    }
}
