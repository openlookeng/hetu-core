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
package io.prestosql.heuristicindex;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.hetu.core.common.heuristicindex.IndexCacheKey;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.Split;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.utils.RangeUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;

public class SplitFiltering
{
    private static final Logger LOG = Logger.get(SplitFiltering.class);
    private static final AtomicLong totalSplitsProcessed = new AtomicLong();
    private static final AtomicLong splitsFiltered = new AtomicLong();
    private static final List<String> INDEX_ORDER = ImmutableList.of("MINMAX", "BLOOM");
    private static final Set<String> PARTITION_INDEX_TYPES = Sets.newHashSet("BTREE");
    private static final String SYMBOL_TABLE_KEY_NAME = "__hetu__symboltable";
    private static final String LAST_MODIFIED_KEY_NAME = "__hetu__lastmodified";
    private static final String MAX_MODIFIED_TIME = "__hetu__maxmodifiedtime";
    private static IndexCache indexCache;

    private SplitFiltering()
    {
    }

    private static synchronized void initCache(IndexClient indexClient)
    {
        if (indexCache == null) {
            CacheLoader<IndexCacheKey, List<IndexMetadata>> cacheLoader = new IndexCacheLoader(indexClient);
            indexCache = new IndexCache(cacheLoader, indexClient);
        }
    }

    public static List<Split> getFilteredSplit(Optional<RowExpression> expression, Optional<String> tableName, Map<Symbol, ColumnHandle> assignments,
            SplitSource.SplitBatch nextSplits, HeuristicIndexerManager heuristicIndexerManager)
    {
        if (!expression.isPresent() || !tableName.isPresent()) {
            return nextSplits.getSplits();
        }

        if (indexCache == null) {
            initCache(heuristicIndexerManager.getIndexClient());
        }

        List<Split> allSplits = nextSplits.getSplits();
        String fullQualifiedTableName = tableName.get();
        long initialSplitsSize = allSplits.size();

        List<IndexRecord> indexRecords = null;
        try {
            indexRecords = heuristicIndexerManager.getIndexClient().getAllIndexRecords();
        }
        catch (IOException e) {
            LOG.debug("Filtering can't be done because not able to read index records", e);
            return allSplits;
        }
        Set<String> referencedColumns = new HashSet<>();
        getAllColumns(expression.get(), referencedColumns, assignments);
        List<IndexRecord> partitionIndexRecords = new ArrayList<>();
        List<IndexRecord> nonPartitionIndexRecords = new ArrayList<>();
        for (IndexRecord indexRecord : indexRecords) {
            if (indexRecord.table.equalsIgnoreCase(fullQualifiedTableName)) {
                List<String> columnsInIndex = Arrays.asList(indexRecord.columns);
                for (String column : referencedColumns) {
                    if (columnsInIndex.contains(column)) {
                        if (PARTITION_INDEX_TYPES.contains(indexRecord.indexType.toUpperCase())) {
                            partitionIndexRecords.add(indexRecord);
                        }
                        else {
                            nonPartitionIndexRecords.add(indexRecord);
                        }
                    }
                }
            }
        }
        List<Split> splitsToReturn = new ArrayList<>();
        if (partitionIndexRecords.isEmpty() && nonPartitionIndexRecords.isEmpty()) {
            return allSplits;
        }
        else if (!partitionIndexRecords.isEmpty() && nonPartitionIndexRecords.isEmpty()) {
            splitsToReturn = filterUsingPartitionIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, heuristicIndexerManager);
        }
        else if (!nonPartitionIndexRecords.isEmpty() && partitionIndexRecords.isEmpty()) {
            splitsToReturn = filterUsingStripeIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, heuristicIndexerManager);
        }
        else {
            // filter using both indexes and return the smallest set of splits.
            List<Split> splitsToReturn1 = filterUsingPartitionIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, heuristicIndexerManager);
            List<Split> splitsToReturn2 = filterUsingStripeIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, heuristicIndexerManager);
            splitsToReturn = splitsToReturn1.size() < splitsToReturn2.size() ? splitsToReturn1 : splitsToReturn2;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("totalSplitsProcessed: " + totalSplitsProcessed.addAndGet(initialSplitsSize));
            LOG.debug("splitsFiltered: " + splitsFiltered.addAndGet(initialSplitsSize - splitsToReturn.size()));
        }

        return splitsToReturn;
    }

    private static List<Split> filterUsingStripeIndex(RowExpression expression, List<Split> inputSplits, String fullQualifiedTableName, Set<String> referencedColumns, HeuristicIndexerManager indexerManager)
    {
        return inputSplits.parallelStream()
                .filter(split -> {
                    Map<String, List<IndexMetadata>> allIndices = new HashMap<>();

                    for (String col : referencedColumns) {
                        List<IndexMetadata> splitIndices = indexCache.getIndices(fullQualifiedTableName, col, split);

                        if (splitIndices == null || splitIndices.size() == 0) {
                            // no index found, keep split
                            continue;
                        }

                        // Group each type of index together and make sure they are sorted in ascending order
                        // with respect to their SplitStart
                        Map<String, List<IndexMetadata>> indexGroupMap = new HashMap<>();
                        for (IndexMetadata splitIndex : splitIndices) {
                            List<IndexMetadata> indexGroup = indexGroupMap.get(splitIndex.getIndex().getId());
                            if (indexGroup == null) {
                                indexGroup = new ArrayList<>();
                                indexGroupMap.put(splitIndex.getIndex().getId(), indexGroup);
                            }

                            insert(indexGroup, splitIndex);
                        }

                        List<String> sortedIndexTypeKeys = new LinkedList<>(indexGroupMap.keySet());
                        sortedIndexTypeKeys.sort(Comparator.comparingInt(e -> INDEX_ORDER.contains(e) ? INDEX_ORDER.indexOf(e) : Integer.MAX_VALUE));

                        for (String indexTypeKey : sortedIndexTypeKeys) {
                            List<IndexMetadata> validIndices = indexGroupMap.get(indexTypeKey);
                            if (validIndices != null) {
                                validIndices = RangeUtil.subArray(validIndices, split.getConnectorSplit().getStartIndex(), split.getConnectorSplit().getEndIndex());
                                List<IndexMetadata> indicesOfCol = allIndices.getOrDefault(col, new LinkedList<>());
                                indicesOfCol.addAll(validIndices);
                                allIndices.put(col, indicesOfCol);
                            }
                        }
                    }

                    if (allIndices.isEmpty()) {
                        return true;
                    }

                    return indexerManager.getIndexFilter(allIndices).matches(expression);
                })
                .collect(Collectors.toList());
    }

    private static List<Split> filterUsingPartitionIndex(RowExpression expression, List<Split> inputSplits, String fullQualifiedTableName, Set<String> referencedColumns, HeuristicIndexerManager indexerManager)
    {
        try {
            long maxLastUpdated = 0L;
            Map<String, List<Split>> partitionSplitMap = new HashMap<>();
            for (Split split : inputSplits) {
                String filePathStr = split.getConnectorSplit().getFilePath();
                long lastUpdated = split.getConnectorSplit().getLastModifiedTime();
                if (lastUpdated > maxLastUpdated) {
                    maxLastUpdated = lastUpdated;
                }
                String partition = getPartitionFromPath(filePathStr);
                if (!partitionSplitMap.containsKey(partition)) {
                    partitionSplitMap.put(partition, new ArrayList<>());
                }
                partitionSplitMap.get(partition).add(split);
            }
            // Split is not compliant to table structure. Return all the splits
            if (partitionSplitMap.isEmpty()) {
                return inputSplits;
            }

            List<Split> result = new ArrayList<>();
            boolean filtered = false;
            for (String column : referencedColumns) {
                List<IndexMetadata> indexMetadataList = new ArrayList<>();
                for (String indexType : PARTITION_INDEX_TYPES) {
                    List<IndexMetadata> output = indexCache.getIndices(fullQualifiedTableName, column, indexType, partitionSplitMap.keySet(), maxLastUpdated);
                    if (output != null && !output.isEmpty()) {
                        indexMetadataList.addAll(output);
                    }
                }

                Map<String, PartitionIndexHolder> partitionIndexHolderMap = new HashMap<>();
                if (indexMetadataList != null && !indexMetadataList.isEmpty()) {
                    for (IndexMetadata indexMetadata : indexMetadataList) {
                        Map<String, String> outputMap = new HashMap<>();
                        Index index = indexMetadata.getIndex();
                        String partition = getPartitionFromPath(indexMetadata.getUri());
                        Iterator iterator = index.lookUp(expression);
                        Properties properties = index.getProperties();
                        String symbolTableStr = properties.getProperty(SYMBOL_TABLE_KEY_NAME);
                        Map<String, String> symbolTable = deserializePropertyToMapString(symbolTableStr);
                        Map<String, Long> lastUpdateTable = deserializePropertyToMapLong(properties.getProperty(LAST_MODIFIED_KEY_NAME));
                        long maxLastUpdate = Long.parseLong(properties.getProperty(MAX_MODIFIED_TIME));
                        while (iterator.hasNext()) {
                            String output = iterator.next().toString();
                            Map<String, String> map = deserializePropertyToMapString(output);
                            outputMap.putAll(map);
                        }
                        partitionIndexHolderMap.putIfAbsent(partition, new PartitionIndexHolder(index, partition, symbolTable, lastUpdateTable, outputMap, maxLastUpdate));
                    }

                    // Start filtering
                    if (!partitionIndexHolderMap.isEmpty()) {
                        for (Map.Entry<String, List<Split>> entry : partitionSplitMap.entrySet()) {
                            String partition = entry.getKey();
                            if (partitionIndexHolderMap.containsKey(partition)) {
                                PartitionIndexHolder partitionIndexHolder = partitionIndexHolderMap.get(partition);
                                Map<String, String> outputMap = partitionIndexHolder.getResultMap();
                                for (Split split : entry.getValue()) {
                                    ConnectorSplit connectorSplit = split.getConnectorSplit();
                                    String filePathStr = connectorSplit.getFilePath();
                                    String fileName = Paths.get(filePathStr).getFileName().toString();
                                    Map<String, String> symbolTable = partitionIndexHolder.getSymbolTable();
                                    if (symbolTable.containsKey(fileName)) {
                                        if (partitionIndexHolder.getLastUpdated().get(fileName) < connectorSplit.getLastModifiedTime()) {
                                            result.add(split);
                                        }
                                        else {
                                            if (outputMap.containsKey(symbolTable.get(fileName))) {
                                                result.add(split);
                                            }
                                        }
                                    }
                                    else {
                                        if (partitionIndexHolder.getMaxLastUpdate() < connectorSplit.getLastModifiedTime()) {
                                            // this is a new split added after index creation
                                            LOG.warn("Looks like index is stale. We found new file" + connectorSplit.getFilePath());
                                            result.add(split);
                                        }
                                    }
                                }
                            }
                            else {
                                result.addAll(entry.getValue());
                            }
                        }
                        filtered = true;
                    }
                }
            }
            return filtered ? result : inputSplits;
        }
        catch (Exception e) {
            //warning any exception in split filtering continue with all splits.
            LOG.debug("Exception occurred while filtering. Returning original splits", e);
            return inputSplits;
        }
    }

    private static String getPartitionFromPath(String filePathStr)
    {
        Path filePath = Paths.get(filePathStr);
        String partitionName = filePath.getName(filePath.getNameCount() - 2).toString();
        return partitionName;
    }

    private static Map<String, String> deserializePropertyToMapString(String property)
    {
        String[] keyValPairs = property.split(",");
        Map<String, String> result = new HashMap<>();
        for (String pair : keyValPairs) {
            if (pair.contains(":")) {
                String[] keyVal = pair.split(":");
                result.put(keyVal[0], keyVal[1]);
            }
        }
        return result;
    }

    private static Map<String, Long> deserializePropertyToMapLong(String property)
    {
        String[] keyValPairs = property.split(",");
        Map<String, Long> result = new HashMap<>();
        for (String pair : keyValPairs) {
            String[] keyVal = pair.split(":");
            result.put(keyVal[0], Long.parseLong(keyVal[1]));
        }
        return result;
    }

    /**
     * Performs list insertion that guarantees SplitStart are sorted in ascending order
     * Cannot assure order when two SplitStarts are the same
     *
     * @param list List to be inserted element obj
     * @param obj SplitIndexMetadata to be inserted to the list
     */
    private static void insert(List<IndexMetadata> list, IndexMetadata obj)
    {
        int listSize = list.size();
        // If there's no element, just insert it
        if (listSize == 0) {
            list.add(obj);
            return;
        }

        long splitStart = obj.getSplitStart();
        for (int i = list.size() - 1; i >= 0; i--) {
            if (list.get(i).getSplitStart() <= splitStart) {
                list.add(i + 1, obj);
                return;
            }
        }
    }

    private static List<PlanNode> getFilterNode(SqlStageExecution stage)
    {
        PlanFragment fragment = stage.getFragment();
        PlanNode root = fragment.getRoot();
        List<PlanNode> result = new LinkedList<>();

        Queue<PlanNode> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof FilterNode
                    || node instanceof TableScanNode) {
                result.add(node);
            }

            queue.addAll(node.getSources());
        }

        return result;
    }

    public static boolean isSplitFilterApplicable(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.isEmpty()) {
            return false;
        }

        PlanNode node = filterNodeOptional.get(0);

        if (node instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) node;
            PlanNode sourceNode = filterNode.getSource();
            if (!(sourceNode instanceof TableScanNode)) {
                return false;
            }

            //if a catalog name starts with a $, it's not an normal query, could be something like show tables;
            TableHandle table = ((TableScanNode) sourceNode).getTable();
            String catalogName = table.getCatalogName().getCatalogName();
            if (catalogName.startsWith("$")) {
                return false;
            }

            /* (!(table.getConnectorHandle().isFilterSupported()
             *   && (isSupportedExpression(filterNode.getPredicate())
             *       || (((TableScanNode) sourceNode).getPredicate().isPresent()
             *           && isSupportedExpression(((TableScanNode) sourceNode).getPredicate().get())))))
             */
            if (!table.getConnectorHandle().isFilterSupported()) {
                return false;
            }

            if (!isSupportedExpression(filterNode.getPredicate())
                    && (!((TableScanNode) sourceNode).getPredicate().isPresent()
                    || !isSupportedExpression(((TableScanNode) sourceNode).getPredicate().get()))) {
                return false;
            }
        }

        if (node instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) node;
            //if a catalog name starts with a $, it's not an normal query, could be something like show tables;
            TableHandle table = tableScanNode.getTable();
            String catalogName = table.getCatalogName().getCatalogName();
            if (catalogName.startsWith("$")) {
                return false;
            }

            if (!table.getConnectorHandle().isFilterSupported()) {
                return false;
            }

            if (!tableScanNode.getPredicate().isPresent()
                    || !isSupportedExpression(tableScanNode.getPredicate().get())) {
                return false;
            }
        }

        return true;
    }

    private static boolean isSupportedExpression(RowExpression predicate)
    {
        if (predicate instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) predicate;
            switch (specialForm.getForm()) {
                case BETWEEN:
                case IN:
                    return true;
                case AND:
                case OR:
                    return isSupportedExpression(specialForm.getArguments().get(0)) && isSupportedExpression(specialForm.getArguments().get(1));
                default:
                    return false;
            }
        }
        if (predicate instanceof CallExpression) {
            CallExpression call = (CallExpression) predicate;
            if (call.getSignature().getName().equals("not")) {
                return true;
            }
            try {
                OperatorType operatorType = call.getSignature().unmangleOperator(call.getSignature().getName());
                if (operatorType.isComparisonOperator() && operatorType != IS_DISTINCT_FROM) {
                    return true;
                }
                return false;
            }
            catch (IllegalArgumentException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Get the expression and column name assignment map, in case some columns are
     * renamed which results in index not loading correctly.
     *
     * @param stage stage object
     * @return Pair of: Expression and a column name assignment map
     */
    public static Tuple<Optional<RowExpression>, Map<Symbol, ColumnHandle>> getExpression(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.size() == 0) {
            return new Tuple<>(Optional.empty(), new HashMap<>());
        }

        if (filterNodeOptional.get(0) instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) filterNodeOptional.get(0);
            if (filterNode.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                if (tableScanNode.getPredicate().isPresent()
                        && isSupportedExpression(tableScanNode.getPredicate().get())) { /* if total filter is not supported use the filterNode */
                    return new Tuple<>(tableScanNode.getPredicate(), tableScanNode.getAssignments());
                }

                return new Tuple<>(Optional.of(filterNode.getPredicate()), tableScanNode.getAssignments());
            }

            return new Tuple<>(Optional.empty(), new HashMap<>());
        }

        if (filterNodeOptional.get(0) instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) filterNodeOptional.get(0);
            if (tableScanNode.getPredicate().isPresent()) {
                return new Tuple<>(tableScanNode.getPredicate(), tableScanNode.getAssignments());
            }
        }

        return new Tuple<>(Optional.empty(), new HashMap<>());
    }

    public static Optional<String> getFullyQualifiedName(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.size() == 0) {
            return Optional.empty();
        }

        TableScanNode tableScanNode;
        if (filterNodeOptional.get(0) instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) filterNodeOptional.get(0);
            tableScanNode = (TableScanNode) filterNode.getSource();
        }
        else {
            tableScanNode = (TableScanNode) filterNodeOptional.get(0);
        }

        String fullQualifiedTableName = tableScanNode.getTable().getFullyQualifiedName();

        return Optional.of(fullQualifiedTableName);
    }

    public static void getAllColumns(RowExpression expression, Set<String> columns, Map<Symbol, ColumnHandle> assignments)
    {
        if (expression instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) expression;
            RowExpression left;
            switch (specialForm.getForm()) {
                case BETWEEN:
                case IN:
                    left = extractExpression(specialForm.getArguments().get(0));
                    break;
                case AND:
                case OR:
                    getAllColumns(specialForm.getArguments().get(0), columns, assignments);
                    getAllColumns(specialForm.getArguments().get(1), columns, assignments);
                    return;
                default:
                    return;
            }
            if (!(left instanceof VariableReferenceExpression)) {
                LOG.warn("Invalid Left of expression %s, should be an VariableReferenceExpression", left.toString());
                return;
            }
            String columnName = ((VariableReferenceExpression) left).getName();
            Symbol columnSymbol = new Symbol(columnName);
            if (assignments.containsKey(columnSymbol)) {
                columnName = assignments.get(columnSymbol).getColumnName();
            }
            columns.add(columnName);
            return;
        }
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            try {
                OperatorType operatorType = call.getSignature().unmangleOperator(call.getSignature().getName());
                if (!operatorType.isComparisonOperator()) {
                    return;
                }
                RowExpression left = extractExpression(call.getArguments().get(0));
                if (!(left instanceof VariableReferenceExpression)) {
                    LOG.warn("Invalid Left of expression %s, should be an VariableReferenceExpression", left.toString());
                    return;
                }
                String columnName = ((VariableReferenceExpression) left).getName();
                Symbol columnSymbol = new Symbol(columnName);
                if (assignments.containsKey(columnSymbol)) {
                    columnName = assignments.get(columnSymbol).getColumnName();
                }
                columns.add(columnName);
                return;
            }
            catch (IllegalArgumentException e) {
                return;
            }
        }
        return;
    }

    private static RowExpression extractExpression(RowExpression expression)
    {
        if (expression instanceof CallExpression && ((CallExpression) expression).getSignature().getName().contains("CAST")) {
            // extract the inner expression for CAST expressions
            return extractExpression(((CallExpression) expression).getArguments().get(0));
        }
        else {
            return expression;
        }
    }

    public static class Tuple<T1, T2>
    {
        public final T1 first;
        public final T2 second;

        public Tuple(T1 v1, T2 v2)
        {
            first = v1;
            second = v2;
        }
    }
}
