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
package io.prestosql.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class TpchSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchSplitManager(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle tableHandle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        if (session.isSnapshotEnabled()) {
            // Snapshot: Modify splits as needed to all them to be scheduled on any node.
            // This allows them to be processed by a different worker after resume.
            List<HostAddress> addresses = nodes.stream().map(Node::getHostAndPort).collect(Collectors.toList());
            for (int i = 0; i < totalParts; i++) {
                splits.add(new TpchSplit(partNumber, totalParts, addresses));
                partNumber++;
            }
        }
        else {
            // Split the data using split and skew by the number of nodes available.
            for (Node node : nodes) {
                for (int i = 0; i < splitsPerNode; i++) {
                    splits.add(new TpchSplit(partNumber, totalParts, ImmutableList.of(node.getHostAndPort())));
                    partNumber++;
                }
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
