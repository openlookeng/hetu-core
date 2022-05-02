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

package io.prestosql.split;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.metadata.Metadata;
import io.prestosql.snapshot.MarkerAnnouncer;
import io.prestosql.snapshot.SnapshotConfig;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SplitManager
{
    private final ConcurrentMap<CatalogName, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;
    // Snapshot: marker announcer for each query. Used to create MarkerSplitSource,
    // and to allow query execution to get a hold of the announcer,
    // e.g. to ask split sources to resume to a snapshot, or to retrieve snapshot related info to schedule ValueNodes
    private final Map<QueryId, MarkerAnnouncer> announcers;

    // NOTE: This only used for filling in the table layout if none is present by the time we
    // get splits. DO NOT USE IT FOR ANY OTHER PURPOSE, as it will be removed once table layouts
    // are gone entirely
    private final Metadata metadata;

    @Inject
    public SplitManager(QueryManagerConfig config, Metadata metadata)
    {
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.metadata = metadata;
        this.announcers = new ConcurrentHashMap<>();
    }

    public void addConnectorSplitManager(CatalogName catalogName, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(catalogName, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorSplitManager(CatalogName catalogName)
    {
        splitManagers.remove(catalogName);
    }

    public SplitSource getSplits(Session session, TableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        return getSplits(session, table, splitSchedulingStrategy, null, Optional.empty(), ImmutableMap.of(), ImmutableSet.of(), false, null);
    }

    public SplitSource getSplits(Session session, TableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Optional<QueryType> queryType, Map<String, Object> queryInfo, Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates,
            boolean partOfReuse, PlanNodeId nodeId)
    {
        MarkerAnnouncer announcer = null;
        if (SystemSessionProperties.isSnapshotEnabled(session)) {
            announcer = getMarkerAnnouncer(session);
            SplitSource splitSource = announcer.getSplitSource(nodeId);
            if (splitSource != null) {
                return splitSource;
            }
        }

        CatalogName catalogName = table.getCatalogName();
        ConnectorSplitManager splitManager = getConnectorSplitManager(catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        ConnectorSplitSource source;
        if (metadata.usesLegacyTableLayouts(session, table)) {
            ConnectorTableLayoutHandle layout = table.getLayout()
                    .orElseGet(() -> metadata.getLayout(session, table, Constraint.alwaysTrue(), Optional.empty())
                            .get()
                            .getNewTableHandle()
                            .getLayout().get());

            source = splitManager.getSplits(table.getTransaction(), connectorSession, layout, splitSchedulingStrategy);
        }
        else {
            source = splitManager.getSplits(table.getTransaction(), connectorSession, table.getConnectorHandle(), splitSchedulingStrategy, dynamicFilterSupplier, queryType, queryInfo, userDefinedCachePredicates, partOfReuse);
        }

        SplitSource splitSource = new ConnectorAwareSplitSource(catalogName, source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        if (SystemSessionProperties.isSnapshotEnabled(session)) {
            splitSource = announcer.createMarkerSplitSource(splitSource, nodeId);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(CatalogName catalogName)
    {
        ConnectorSplitManager result = splitManagers.get(catalogName);
        checkArgument(result != null, "No split manager for connector '%s'", catalogName);
        return result;
    }

    public void queryFinished(QueryId queryId)
    {
        announcers.remove(queryId);
    }

    public MarkerAnnouncer getMarkerAnnouncer(Session session)
    {
        return announcers.computeIfAbsent(session.getQueryId(), queryId -> {
            if (SystemSessionProperties.getSnapshotIntervalType(session) == SnapshotConfig.IntervalType.TIME) {
                return new MarkerAnnouncer(SystemSessionProperties.getSnapshotTimeInterval(session));
            }
            else {
                return new MarkerAnnouncer(SystemSessionProperties.getSnapshotSplitCountInterval(session));
            }
        });
    }
}
