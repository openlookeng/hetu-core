/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.airlift.slice.Slice;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexWriter;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.heuristicindex.TypeUtils.getActualValue;
import static java.util.Objects.requireNonNull;

public class CreateIndexOperator
        implements Operator
{
    private final Map<CreateIndexOperator, Boolean> finished;
    private final Map<String, IndexWriter> levelWriter;
    private final Map<IndexWriter, CreateIndexOperator> persistBy;
    private final OperatorContext operatorContext;
    private final CreateIndexMetadata createIndexMetadata;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private static final Logger LOG = Logger.get(CreateIndexOperator.class);

    public CreateIndexOperator(
            OperatorContext operatorContext,
            CreateIndexMetadata createIndexMetadata,
            HeuristicIndexerManager heuristicIndexerManager,
            Map<String, IndexWriter> levelWriter,
            Map<IndexWriter, CreateIndexOperator> persistBy,
            Map<CreateIndexOperator, Boolean> finished)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.createIndexMetadata = requireNonNull(createIndexMetadata, "createIndexMetadata is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.levelWriter = requireNonNull(levelWriter, "levelWriter is null");
        this.persistBy = requireNonNull(persistBy, "persisted is null");
        this.finished = requireNonNull(finished, "finished is null");
    }

    private State state = State.NEEDS_INPUT;

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
        if (state != State.NEEDS_INPUT) {
            return;
        }

        // start PERSISTING
        // if this operator is responsible for PERSISTING an IndexWriter it will call persist()
        // otherwise the operator will simply go from PERSISTING to FINISHED_PERSISTING without calling persist()
        state = State.PERSISTING;

        // mark current operator as finished
        finished.put(this, true);

        // wait for all operators to have their finish() method called, i.e. there's no more inputs expected
        while (finished.containsValue(false)) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            }
            catch (InterruptedException e) {
                throw new RuntimeException("CreateIndexOperator unexpectedly interrupted while waiting for all operators to finish: ", e);
            }
        }

        // persist index to disk if this operator is responsible for persisting a writer
        try {
            Iterator<Map.Entry<String, IndexWriter>> iterator = levelWriter.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, IndexWriter> entry = iterator.next();
                if (persistBy.get(entry.getValue()) == this) {
                    String writerKey = entry.getKey();
                    entry.getValue().persist();
                    iterator.remove(); // remove reference to writer once persisted so it can be GCed
                    LOG.debug("Writer for %s has finished persisting. Remaining: %d", writerKey, levelWriter.size());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Persisting index failed: ", e);
        }

        synchronized (levelWriter) {
            // All writers have finished persisting
            if (levelWriter.isEmpty()) {
                LOG.debug("Writing index record by %s", this);
                if (persistBy.isEmpty()) {
                    // table scan is empty. no data scanned from table. addInput() has never been called.
                    throw new IllegalStateException("The table is empty. No index will be created.");
                }

                // update metadata
                IndexClient indexClient = heuristicIndexerManager.getIndexClient();
                try {
                    IndexClient.RecordStatus recordStatus = indexClient.lookUpIndexRecord(createIndexMetadata);
                    LOG.debug("Current record status: %s", recordStatus);

                    switch (recordStatus) {
                        case SAME_NAME:
                        case IN_PROGRESS_SAME_NAME:
                        case IN_PROGRESS_SAME_CONTENT:
                        case IN_PROGRESS_SAME_INDEX_PART_CONFLICT:
                        case IN_PROGRESS_SAME_INDEX_PART_CAN_MERGE:
                            indexClient.deleteIndexRecord(createIndexMetadata.getIndexName(), Collections.emptyList());
                            indexClient.addIndexRecord(createIndexMetadata);
                            break;
                        case NOT_FOUND:
                        case SAME_INDEX_PART_CAN_MERGE:
                            indexClient.addIndexRecord(createIndexMetadata);
                            break;
                        default:
                    }
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Unable to update index records: ", e);
                }
            }
        }

        state = State.FINISHED_PERSISTING;
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

        // if operator is still receiving input, it's not finished
        finished.putIfAbsent(this, false);

        if (page.getPositionCount() == 0) {
            return;
        }

        if (createIndexMetadata.getCreateLevel() == CreateIndexMetadata.Level.UNDEFINED) {
            boolean tableIsPartitioned = getPartitionName(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_PATH),
                    createIndexMetadata.getTableName()) != null;
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
                    levelWriter.computeIfAbsent(filePath, k -> heuristicIndexerManager.getIndexWriter(createIndexMetadata, connectorMetadata));
                    persistBy.putIfAbsent(levelWriter.get(filePath), this);
                    levelWriter.get(filePath).addData(values, connectorMetadata);
                    break;
                }
                case PARTITION: {
                    String partition = getPartitionName(page.getPageMetadata().getProperty(HetuConstant.DATASOURCE_FILE_PATH),
                            createIndexMetadata.getTableName());
                    if (partition == null) {
                        throw new IllegalStateException("Partition level is not supported for non partitioned table.");
                    }
                    levelWriter.putIfAbsent(partition, heuristicIndexerManager.getIndexWriter(createIndexMetadata, connectorMetadata));
                    persistBy.putIfAbsent(levelWriter.get(partition), this);
                    levelWriter.get(partition).addData(values, connectorMetadata);
                    break;
                }
                case TABLE: {
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
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public Page getOutput()
    {
        if (state != State.FINISHED_PERSISTING) {
            return null;
        }

        state = State.FINISHED;
        return null;
    }

    public static class CreateIndexOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final CreateIndexMetadata createIndexMetadata;
        private final HeuristicIndexerManager heuristicIndexerManager;
        private final Map<String, IndexWriter> levelWriter;
        private final Map<IndexWriter, CreateIndexOperator> persistBy;
        private final Map<CreateIndexOperator, Boolean> finished;
        private boolean closed;

        public CreateIndexOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                CreateIndexMetadata createIndexMetadata,
                HeuristicIndexerManager heuristicIndexerManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.createIndexMetadata = createIndexMetadata;
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
            this.levelWriter = new ConcurrentHashMap<>();
            this.persistBy = new ConcurrentHashMap<>();
            this.finished = new ConcurrentHashMap<>();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, CreateIndexOperator.class.getSimpleName());
            return new CreateIndexOperator(operatorContext, createIndexMetadata, heuristicIndexerManager, levelWriter, persistBy, finished);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CreateIndexOperatorFactory(operatorId, planNodeId, createIndexMetadata, heuristicIndexerManager);
        }
    }

    private static Object getNativeValue(Type type, Block block, int position)
    {
        Object obj = TypeUtils.readNativeValue(type, block, position);
        Class<?> javaType = type.getJavaType();

        if (obj != null && javaType == Slice.class) {
            obj = ((Slice) obj).toStringUtf8();
        }

        return obj;
    }

    /**
     * This is a hacky way to tell if the table is partitioned and get the partition of table
     * <p>
     * Should be replaced if a better solution is available
     *
     * @param uri page data path uri
     * @param tableName table name
     * @return partition name if table is partitioned. {@code null} if no partition is found in the path.
     */
    private static String getPartitionName(String uri, String tableName)
    {
        Path path = Paths.get(uri);
        String[] dataPathElements = path.toString().split("/");
        String[] fullTableName = tableName.split("\\.");
        String simpleTableName = fullTableName[fullTableName.length - 1];
        for (int i = 0; i < dataPathElements.length; i++) {
            if (dataPathElements[i].equalsIgnoreCase(simpleTableName) && dataPathElements[i + 1].contains("=")) {
                return dataPathElements[i + 1];
            }
        }

        return null;
    }
}
