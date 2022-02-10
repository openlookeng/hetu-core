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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.DROP_TABLE_ON_CUBE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class DropTableTask
        implements DataDefinitionTask<DropTable>
{
    private static final Logger LOG = Logger.get(DropTableTask.class);
    private final CubeManager cubeManager;

    @Inject
    public DropTableTask(CubeManager cubeManager)
    {
        this.cubeManager = cubeManager;
    }

    @Override
    public String getName()
    {
        return "DROP TABLE";
    }

    @Override
    public ListenableFuture<?> execute(DropTable statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        Session session = stateMachine.getSession();

        QualifiedObjectName fullObjectName = createQualifiedObjectName(session, statement, statement.getTableName());
        QualifiedName tableName = QualifiedName.of(fullObjectName.getCatalogName(), fullObjectName.getSchemaName(), fullObjectName.getObjectName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, fullObjectName);
        if (!tableHandle.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        Optional<CubeMetaStore> optionalCubeMetaStore = this.cubeManager.getMetaStore(STAR_TREE);
        if (optionalCubeMetaStore.isPresent() && optionalCubeMetaStore.get().getMetadataFromCubeName(tableName.toString()).isPresent()) {
            throw new SemanticException(DROP_TABLE_ON_CUBE, statement, "%s is a star-tree cube, drop using DROP CUBE", tableName);
        }

        accessControl.checkCanDropTable(session.getRequiredTransactionId(), session.getIdentity(), fullObjectName);

        if (PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
            // Check if SplitCacheMap is enabled
            SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();
            if (splitCacheMap.cacheExists(tableName)) {
                splitCacheMap.dropCache(tableName, Optional.empty());
            }
        }

        metadata.dropTable(session, tableHandle.get());

        if (optionalCubeMetaStore.isPresent()) {
            List<CubeMetadata> cubes = optionalCubeMetaStore.get().getMetadataList(tableName.toString());
            for (CubeMetadata cube : cubes) {
                String[] parts = cube.getCubeName().split("\\.");
                Optional<TableHandle> cubeHandle = metadata.getTableHandle(session, createQualifiedObjectName(session, null, QualifiedName.of(parts[0], parts[1], parts[2])));
                try {
                    cubeHandle.ifPresent(cubeTable -> metadata.dropTable(session, cubeTable));
                    optionalCubeMetaStore.get().removeCube(cube);
                }
                catch (TableNotFoundException e) {
                    // Can happen in concurrent drop table and drop cube calls
                    LOG.debug("Tried dropping cube table but it is already dropped", e);
                }
            }
        }

        dropIndices(heuristicIndexerManager, tableName);

        return immediateFuture(null);
    }

    private void dropIndices(HeuristicIndexerManager heuristicIndexerManager, QualifiedName tableName)
    {
        IndexClient indexClient = heuristicIndexerManager.getIndexClient();
        try {
            List<IndexRecord> indexRecords = indexClient.getAllIndexRecords().stream().filter(r -> r.qualifiedTable.equals(tableName.toString())).collect(Collectors.toList());
            for (IndexRecord indexRecord : indexRecords) {
                indexClient.deleteIndex(indexRecord.name, Collections.emptyList());
            }
        }
        catch (UnsupportedOperationException ignored) {
            // This exception is only thrown when heuristic index is not enabled so the noOpIndexClient is used.
            // In this case we want drop table to run as normal
            LOG.debug("heuristic index is not enabled, then noOpIndexClient is used: %s", ignored);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
