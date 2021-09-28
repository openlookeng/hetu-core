/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.operator;

import io.airlift.log.Logger;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.connector.UpdateIndexMetadata;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.heuristicindex.IndexWriter;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.CreateIndexOperator.getNativeValue;
import static io.prestosql.operator.CreateIndexOperator.getPartitionName;
import static io.prestosql.spi.heuristicindex.TypeUtils.getActualValue;
import static java.util.Objects.requireNonNull;

@RestorableConfig(unsupported = true)
public class UpdateIndexOperator
        implements Operator
{
    private final Map<String, String> pathToModifiedTime;
    private final Map<UpdateIndexOperator, Boolean> finished;
    private final Map<String, IndexWriter> levelWriter;
    private final Map<IndexWriter, UpdateIndexOperator> persistBy;
    private final Map<String, Long> indexLevelToMaxModifiedTime;
    private final OperatorContext operatorContext;
    private final CreateIndexMetadata createIndexMetadata;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private static final Logger LOG = Logger.get(UpdateIndexOperator.class);

    public UpdateIndexOperator(
            OperatorContext operatorContext,
            CreateIndexMetadata createIndexMetadata,
            HeuristicIndexerManager heuristicIndexerManager,
            Map<String, String> pathToModifiedTime,
            Map<String, Long> indexLevelToMaxModifiedTime,
            Map<String, IndexWriter> levelWriter,
            Map<IndexWriter, UpdateIndexOperator> persistBy,
            Map<UpdateIndexOperator, Boolean> finished)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.createIndexMetadata = requireNonNull(createIndexMetadata, "createIndexMetadata is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.pathToModifiedTime = requireNonNull(pathToModifiedTime, "pathToModifiedTime is null");
        this.indexLevelToMaxModifiedTime = requireNonNull(indexLevelToMaxModifiedTime, "partitionToMaxModifiedTime is null");
        this.levelWriter = requireNonNull(levelWriter, "levelWriter is null");
        this.persistBy = requireNonNull(persistBy, "persisted is null");
        this.finished = requireNonNull(finished, "finished is null");
    }

    private UpdateIndexOperator.State state = UpdateIndexOperator.State.NEEDS_INPUT;

    // NEEDS_INPUT -> PERSISTING -> FINISHED_PERSISTING -> FINISHED
    private enum State
    {
        NEEDS_INPUT,
        PERSISTING,
        FINISHED_PERSISTING,
        FINISHED
    }

    @Override
    public void finish()
    {
        // if state isn't NEEDS_INPUT, it means finish was previously called,
        // i.e. index is PERSISTING , FINISHED_PERSISTING or FINISHED
        if (state != UpdateIndexOperator.State.NEEDS_INPUT) {
            return;
        }

        // start PERSISTING
        // if this operator is responsible for PERSISTING an IndexWriter it will call persist()
        // otherwise the operator will simply go from PERSISTING to FINISHED_PERSISTING without calling persist()
        state = UpdateIndexOperator.State.PERSISTING;

        // mark current operator as finished
        finished.put(this, true);

        // wait for all operators to have their finish() method called, i.e. there's no more inputs expected
        while (finished.containsValue(false)) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            }
            catch (InterruptedException e) {
                throw new RuntimeException("UpdateIndexOperator unexpectedly interrupted while waiting for all operators to finish: ", e);
            }
        }

        // persist index to disk if this operator is responsible for persisting a writer
        try {
            Iterator<Map.Entry<String, IndexWriter>> iterator = levelWriter.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, IndexWriter> entry = iterator.next();

                if (persistBy.get(entry.getValue()) == this) {
                    // A partition/table index doesn't need to be updated as none of the orc files in the partition have a newer modified time
                    if (createIndexMetadata.getCreateLevel() == CreateIndexMetadata.Level.STRIPE ||
                            !(pathToModifiedTime.containsKey(entry.getKey()) &&
                                    indexLevelToMaxModifiedTime.containsKey(entry.getKey()) &&
                                    indexLevelToMaxModifiedTime.get(entry.getKey()) <= Long.parseLong(pathToModifiedTime.get(entry.getKey())))) {
                        entry.getValue().persist();
                    }
                    String writerKey = entry.getKey();
                    iterator.remove(); // remove reference to writer once persisted so it can be GCed
                    LOG.debug("Writer for %s has finished persisting. Remaining: %d", writerKey, levelWriter.size());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Persisting index failed: " + e.getMessage(), e);
        }

        synchronized (levelWriter) {
            // All writers have finished persisting
            if (levelWriter.isEmpty()) {
                LOG.debug("Writing index record by %s", this);

                // update metadata
                IndexClient indexClient = heuristicIndexerManager.getIndexClient();
                try {
                    createIndexMetadata.setIndexSize(heuristicIndexerManager.getIndexClient().getIndexSize(createIndexMetadata.getIndexName()));
                    IndexClient.RecordStatus recordStatus = indexClient.lookUpIndexRecord(createIndexMetadata);
                    LOG.debug("Current record status: %s", recordStatus);
                    CreateIndexMetadata updatedCreateIndexMetadata = new CreateIndexMetadata(
                            createIndexMetadata.getIndexName(),
                            createIndexMetadata.getTableName(),
                            createIndexMetadata.getIndexType(),
                            createIndexMetadata.getIndexSize(),
                            createIndexMetadata.getIndexColumns(),
                            createIndexMetadata.getPartitions(),
                            createIndexMetadata.getProperties(),
                            createIndexMetadata.getUser(),
                            createIndexMetadata.getCreateLevel());
                    indexClient.deleteIndexRecord(updatedCreateIndexMetadata.getIndexName(), Collections.emptyList());
                    indexClient.addIndexRecord(updatedCreateIndexMetadata);
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Unable to update index records: ", e);
                }
            }
        }

        state = UpdateIndexOperator.State.FINISHED_PERSISTING;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        //TODO-cp-I38S9O: Operator currently not supported for Snapshot
        if (page instanceof MarkerPage) {
            throw new UnsupportedOperationException("Operator doesn't support snapshotting.");
        }

        // if operator is still receiving input, it's not finished
        finished.putIfAbsent(this, false);

        if (page.getPositionCount() == 0) {
            return;
        }

        IndexRecord indexRecord;
        try {
            indexRecord = heuristicIndexerManager.getIndexClient().lookUpIndexRecord(createIndexMetadata.getIndexName());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error reading index records, ", e);
        }

        if (createIndexMetadata.getCreateLevel() == CreateIndexMetadata.Level.UNDEFINED) {
            boolean tableIsPartitioned = getPartitionName(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_PATH),
                    indexRecord.qualifiedTable) != null;
            createIndexMetadata.decideIndexLevel(tableIsPartitioned);
        }

        Map<String, List<Object>> values = new HashMap<>();

        for (int blockId = 0; blockId < page.getChannelCount(); blockId++) {
            Block block = page.getBlock(blockId);
            Pair<String, Type> entry = createIndexMetadata.getIndexColumns().get(blockId);
            String indexColumn = entry.getFirst();
            Type type = entry.getSecond();

            for (int position = 0; position < block.getPositionCount(); ++position) {
                Object value = getNativeValue(type, block, position);
                value = getActualValue(type, value);
                values.computeIfAbsent(indexColumn, k -> new ArrayList<>()).add(value);
            }
        }

        Properties connectorMetadata = new Properties();
        connectorMetadata.put(HetuConstant.DATASOURCE_CATALOG, createIndexMetadata.getTableName().split("\\.")[0]);
        connectorMetadata.putAll(page.getPageMetadata());
        try {
            switch (createIndexMetadata.getCreateLevel()) {
                case STRIPE: {
                    String filePath = page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_PATH);
                    // The orc file this page resides in wasn't modified from when the index was created/last updated
                    if (pathToModifiedTime.containsKey(filePath) &&
                            pathToModifiedTime.get(filePath).equals(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION))) {
                        return;
                    }
                    levelWriter.computeIfAbsent(filePath, k -> heuristicIndexerManager.getIndexWriter(createIndexMetadata, connectorMetadata));
                    persistBy.putIfAbsent(levelWriter.get(filePath), this);
                    levelWriter.get(filePath).addData(values, connectorMetadata);
                    break;
                }
                case PARTITION: {
                    String partition = getPartitionName(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_PATH),
                            createIndexMetadata.getTableName());
                    indexLevelToMaxModifiedTime.compute(partition, (k, v) -> {
                        if (v != null && v >= (Long.parseLong(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION)))) {
                            return v;
                        }
                        return (Long.parseLong(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION)));
                    });
                    levelWriter.putIfAbsent(partition, heuristicIndexerManager.getIndexWriter(createIndexMetadata, connectorMetadata));
                    persistBy.putIfAbsent(levelWriter.get(partition), this);
                    levelWriter.get(partition).addData(values, connectorMetadata);
                    break;
                }
                case TABLE: {
                    indexLevelToMaxModifiedTime.compute(createIndexMetadata.getTableName(), (k, v) -> {
                        if (v != null && v >= (Long.parseLong(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION)))) {
                            return v;
                        }
                        return (Long.parseLong(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION)));
                    });
                    levelWriter.putIfAbsent(createIndexMetadata.getTableName(), heuristicIndexerManager.getIndexWriter(createIndexMetadata, connectorMetadata));
                    persistBy.putIfAbsent(levelWriter.get(createIndexMetadata.getTableName()), this);
                    levelWriter.get(createIndexMetadata.getTableName()).addData(values, connectorMetadata);
                    break;
                }
                default:
                    throw new IllegalArgumentException("Create level not supported");
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == UpdateIndexOperator.State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == UpdateIndexOperator.State.NEEDS_INPUT;
    }

    @Override
    public Page getOutput()
    {
        if (state != UpdateIndexOperator.State.FINISHED_PERSISTING) {
            return null;
        }

        state = UpdateIndexOperator.State.FINISHED;
        return null;
    }

    @Override
    public Page pollMarker()
    {
        //TODO-cp-I38S9O: Operator currently not supported for Snapshot
        return null;
    }

    public static class UpdateIndexOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final CreateIndexMetadata createIndexMetadata;
        private final UpdateIndexMetadata updateIndexMetadata;
        private final HeuristicIndexerManager heuristicIndexerManager;
        private final Map<String, String> pathToModifiedTime;
        // Only used for Partition and Table type indices
        private final Map<String, Long> indexLevelToMaxModifiedTime;
        private final Map<String, IndexWriter> levelWriter;
        private final Map<IndexWriter, UpdateIndexOperator> persistBy;
        private final Map<UpdateIndexOperator, Boolean> finished;
        private boolean closed;

        public UpdateIndexOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                UpdateIndexMetadata updateIndexMetadata,
                HeuristicIndexerManager heuristicIndexerManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

            IndexRecord indexRecord;
            try {
                indexRecord = heuristicIndexerManager.getIndexClient().lookUpIndexRecord(updateIndexMetadata.getIndexName());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading index records, ", e);
            }
            this.updateIndexMetadata = updateIndexMetadata;

            // any new properties from UpdateIndexMetadata should override the existing IndexRecord's properties
            // except the level property, since that can not be changed after
            Properties updatedProperties = indexRecord.getProperties();
            updateIndexMetadata.getProperties().forEach((key, val) -> {
                if (!key.toString().toLowerCase(Locale.ROOT).equals("level")) {
                    updatedProperties.setProperty(key.toString(), val.toString());
                }
            });

            this.createIndexMetadata = new CreateIndexMetadata(
                    indexRecord.name,
                    indexRecord.qualifiedTable,
                    indexRecord.indexType,
                    indexRecord.indexSize,
                    Arrays.stream(indexRecord.columns).map(col -> new Pair<>(col,
                            updateIndexMetadata.getColumnTypes().get(col.toLowerCase(Locale.ROOT)))).collect(Collectors.toList()),
                    indexRecord.partitions,
                    updatedProperties,
                    updateIndexMetadata.getUser(),
                    indexRecord.getLevel());

            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
            try {
                this.pathToModifiedTime = heuristicIndexerManager.getIndexClient().getLastModifiedTimes(createIndexMetadata.getIndexName());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error retrieving mapping of orc file name to modified time, ", e);
            }

            this.indexLevelToMaxModifiedTime = new ConcurrentHashMap<>();
            this.levelWriter = new ConcurrentHashMap<>();
            this.persistBy = new ConcurrentHashMap<>();
            this.finished = new ConcurrentHashMap<>();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, UpdateIndexOperator.class.getSimpleName());

            return new UpdateIndexOperator(operatorContext, createIndexMetadata, heuristicIndexerManager, pathToModifiedTime, indexLevelToMaxModifiedTime, levelWriter, persistBy, finished);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new UpdateIndexOperator.UpdateIndexOperatorFactory(operatorId, planNodeId, updateIndexMetadata, heuristicIndexerManager);
        }
    }
}
