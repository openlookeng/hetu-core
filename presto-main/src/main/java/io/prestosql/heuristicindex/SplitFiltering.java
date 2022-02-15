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
package io.prestosql.heuristicindex;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.IndexCacheKey;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexLookUpException;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.heuristicindex.SerializationUtils;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.utils.RangeUtil;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.heuristicindex.SerializationUtils.deserializeStripeSymbol;

public class SplitFiltering
{
    private static final Logger LOG = Logger.get(SplitFiltering.class);
    private static final AtomicLong totalSplitsProcessed = new AtomicLong();
    private static final AtomicLong splitsFiltered = new AtomicLong();
    private static final List<String> FORWARD_INDEX = ImmutableList.of("MINMAX", "BLOOM");
    private static final Set<String> INVERTED_INDEX = Sets.newHashSet("BTREE");
    private static final String MAX_MODIFIED_TIME = "__hetu__maxmodifiedtime";
    private static final String TABLE_LEVEL_KEY = "__index__is__table__level__";
    private static final String PRELOAD_ALL_KEY = "ALL";

    private static IndexCache indexCache;

    private SplitFiltering()
    {
    }

    private static synchronized void initCache(IndexClient indexClient)
    {
        CacheLoader<IndexCacheKey, List<IndexMetadata>> cacheLoader = new IndexCacheLoader(indexClient);
        indexCache = new IndexCache(cacheLoader, indexClient);
    }

    public static IndexCache getCache(IndexClient indexClient)
    {
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            if (indexCache == null) {
                initCache(indexClient);
            }
            return indexCache;
        }
        else {
            throw new IllegalStateException(HetuConstant.HINDEX_CONFIG_ERROR_MSG);
        }
    }

    public static void preloadCache(IndexClient indexClient, List<String> preloadIndexNames)
            throws IOException
    {
        List<IndexRecord> indexToPreload = new ArrayList<>(preloadIndexNames.size());

        if (preloadIndexNames.contains(PRELOAD_ALL_KEY)) {
            indexToPreload = indexClient.getAllIndexRecords();
            LOG.info("Preloading all indices: " + indexToPreload.stream().map(r -> r.name).collect(Collectors.joining(",")));
        }
        else {
            for (String indexName : preloadIndexNames) {
                IndexRecord record = indexClient.lookUpIndexRecord(indexName);
                if (record != null) {
                    indexToPreload.add(indexClient.lookUpIndexRecord(indexName));
                }
                else {
                    LOG.info("Index " + indexName + " is not found. Preloading skipped.");
                }
            }
        }

        for (IndexRecord record : indexToPreload) {
            LOG.info("Preloading index %s to cache...", record.name);
            Duration timeElapsed = indexCache.loadIndexToCache(record);
            LOG.info("Index %s was loaded to cache. (Time elapsed: %s)", record.name, timeElapsed.toString());
        }
    }

    public static List<Split> getFilteredSplit(Optional<RowExpression> expression, Optional<String> tableName, Map<Symbol, ColumnHandle> assignments,
            SplitSource.SplitBatch nextSplits, HeuristicIndexerManager heuristicIndexerManager)
    {
        if (!expression.isPresent() || !tableName.isPresent()) {
            return nextSplits.getSplits();
        }

        List<Split> allSplits = nextSplits.getSplits();
        String fullQualifiedTableName = tableName.get();
        long initialSplitsSize = allSplits.size();

        List<IndexRecord> indexRecords;
        try {
            indexRecords = heuristicIndexerManager.getIndexClient().getAllIndexRecords();
        }
        catch (IOException e) {
            LOG.debug("Filtering can't be done because not able to read index records", e);
            return allSplits;
        }
        Set<String> referencedColumns = new HashSet<>();
        getAllColumns(expression.get(), referencedColumns, assignments);
        Map<String, IndexRecord> forwardIndexRecords = new HashMap<>();
        Map<String, IndexRecord> invertedIndexRecords = new HashMap<>();
        for (IndexRecord indexRecord : indexRecords) {
            if (indexRecord.qualifiedTable.equalsIgnoreCase(fullQualifiedTableName)) {
                List<String> columnsInIndex = Arrays.asList(indexRecord.columns);
                for (String column : referencedColumns) {
                    if (columnsInIndex.contains(column)) {
                        String indexRecordKey = indexRecord.qualifiedTable + "/" + column + "/" + indexRecord.indexType;
                        if (INVERTED_INDEX.contains(indexRecord.indexType.toUpperCase())) {
                            forwardIndexRecords.put(indexRecordKey, indexRecord);
                        }
                        else {
                            invertedIndexRecords.put(indexRecordKey, indexRecord);
                        }
                    }
                }
            }
        }
        List<Split> splitsToReturn;
        if (forwardIndexRecords.isEmpty() && invertedIndexRecords.isEmpty()) {
            return allSplits;
        }
        else if (!forwardIndexRecords.isEmpty() && invertedIndexRecords.isEmpty()) {
            splitsToReturn = filterUsingInvertedIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, forwardIndexRecords, heuristicIndexerManager);
        }
        else if (!invertedIndexRecords.isEmpty() && forwardIndexRecords.isEmpty()) {
            splitsToReturn = filterUsingForwardIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, invertedIndexRecords, heuristicIndexerManager);
        }
        else {
            // filter using both indexes and return the smallest set of splits.
            List<Split> splitsToReturn1 = filterUsingInvertedIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, forwardIndexRecords, heuristicIndexerManager);
            List<Split> splitsToReturn2 = filterUsingForwardIndex(expression.get(), allSplits, fullQualifiedTableName, referencedColumns, invertedIndexRecords, heuristicIndexerManager);
            splitsToReturn = splitsToReturn1.size() < splitsToReturn2.size() ? splitsToReturn1 : splitsToReturn2;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("totalSplitsProcessed: " + totalSplitsProcessed.addAndGet(initialSplitsSize));
            LOG.debug("splitsFiltered: " + splitsFiltered.addAndGet(initialSplitsSize - splitsToReturn.size()));
        }

        return splitsToReturn;
    }

    private static List<Split> filterUsingForwardIndex(RowExpression expression, List<Split> inputSplits, String fullQualifiedTableName, Set<String> referencedColumns, Map<String, IndexRecord> indexRecordKeyToRecordMap, HeuristicIndexerManager indexerManager)
    {
        return inputSplits.parallelStream()
                .filter(split -> {
                    Map<String, List<IndexMetadata>> allIndices = new HashMap<>();

                    for (String col : referencedColumns) {
                        List<IndexMetadata> splitIndices = getCache(indexerManager.getIndexClient()).getIndices(fullQualifiedTableName, col, split, indexRecordKeyToRecordMap);

                        if (splitIndices == null || splitIndices.size() == 0) {
                            // no index found, keep split
                            continue;
                        }

                        // Group each type of index together and make sure they are sorted in ascending order
                        // with respect to their SplitStart
                        Map<String, List<IndexMetadata>> indexGroupMap = new HashMap<>();
                        for (IndexMetadata splitIndex : splitIndices) {
                            List<IndexMetadata> indexGroup = indexGroupMap.computeIfAbsent(splitIndex.getIndex().getId(), k -> new ArrayList<>());
                            insert(indexGroup, splitIndex);
                        }

                        List<String> sortedIndexTypeKeys = new LinkedList<>(indexGroupMap.keySet());
                        sortedIndexTypeKeys.sort(Comparator.comparingInt(e -> FORWARD_INDEX.contains(e) ? FORWARD_INDEX.indexOf(e) : Integer.MAX_VALUE));

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

    private static List<Split> filterUsingInvertedIndex(RowExpression expression, List<Split> inputSplits, String fullQualifiedTableName, Set<String> referencedColumns, Map<String, IndexRecord> indexRecordKeyToRecordMap, HeuristicIndexerManager indexerManager)
    {
        try {
            Map<String, Long> inputMaxLastUpdated = new HashMap<>();
            Map<String, Long> indexMaxLastUpdated = new HashMap<>();
            Map<String, List<Split>> partitionSplitMap = new HashMap<>();

            for (Split split : inputSplits) {
                String filePathStr = split.getConnectorSplit().getFilePath();
                String indexKey = getPartitionKeyOrElse(filePathStr, TABLE_LEVEL_KEY);

                long lastUpdated = split.getConnectorSplit().getLastModifiedTime();
                if (!inputMaxLastUpdated.containsKey(indexKey) || lastUpdated > inputMaxLastUpdated.get(indexKey)) {
                    inputMaxLastUpdated.put(indexKey, lastUpdated);
                }
                if (!partitionSplitMap.containsKey(indexKey)) {
                    partitionSplitMap.put(indexKey, new ArrayList<>());
                }
                partitionSplitMap.get(indexKey).add(split);
            }

            // Split is not compliant to table structure. Return all the splits
            if (partitionSplitMap.isEmpty()) {
                return inputSplits;
            }

            Map<String, List<IndexMetadata>> allIndices = new HashMap<>(); // col -> list of all indices on this column (all partitions)

            // index loading and verification
            for (String column : referencedColumns) {
                List<IndexMetadata> indexMetadataList = new ArrayList<>();

                for (String indexType : INVERTED_INDEX) {
                    indexMetadataList.addAll(getCache(indexerManager.getIndexClient()).getIndices(fullQualifiedTableName, column, indexType,
                            partitionSplitMap.keySet(), Collections.max(inputMaxLastUpdated.values()), indexRecordKeyToRecordMap));
                }

                // If any of the split contains data which is modified after the index was created, return without filtering
                for (IndexMetadata index : indexMetadataList) {
                    String partitionKey = getPartitionKeyOrElse(index.getUri(), TABLE_LEVEL_KEY);
                    long lastModifiedTime = Long.parseLong(index.getIndex().getProperties().getProperty(MAX_MODIFIED_TIME));
                    indexMaxLastUpdated.put(partitionKey, lastModifiedTime);
                }

                allIndices.put(column, indexMetadataList);
            }

            // lookup index
            IndexFilter filter = indexerManager.getIndexFilter(allIndices);
            Iterator<String> iterator = filter.lookUp(expression);
            if (iterator == null) {
                throw new IndexLookUpException();
            }
            Map<String, List<Pair<Long, Long>>> lookUpResults = new HashMap<>(); // all positioned looked up from index, organized by file path
            while (iterator.hasNext()) {
                SerializationUtils.LookUpResult parsedLookUpResult = deserializeStripeSymbol(iterator.next());
                if (!lookUpResults.containsKey(parsedLookUpResult.filepath)) {
                    lookUpResults.put(parsedLookUpResult.filepath, new ArrayList<>());
                }
                lookUpResults.get(parsedLookUpResult.filepath).add(parsedLookUpResult.stripe);
            }

            // filtering
            List<Split> filteredSplits = new ArrayList<>();
            for (Map.Entry<String, List<Split>> entry : partitionSplitMap.entrySet()) {
                String partitionKey = entry.getKey();

                // the partition is indexed by its own partition's index
                boolean partitionHasOwnIndex = indexMaxLastUpdated.containsKey(partitionKey);
                // the partition is covered by a table-level index
                boolean partitionHasTableLevelIndex = indexMaxLastUpdated.size() == 1 && indexMaxLastUpdated.containsKey(TABLE_LEVEL_KEY);

                if (!partitionHasOwnIndex && !partitionHasTableLevelIndex) {
                    filteredSplits.addAll(entry.getValue());
                }
                else {
                    long indexLastModifiedTimeOfThisPartition;
                    if (partitionHasOwnIndex) {
                        indexLastModifiedTimeOfThisPartition = indexMaxLastUpdated.get(partitionKey);
                    }
                    else {
                        indexLastModifiedTimeOfThisPartition = indexMaxLastUpdated.get(TABLE_LEVEL_KEY);
                    }

                    for (Split split : entry.getValue()) {
                        String filePathStr = new URI(split.getConnectorSplit().getFilePath()).getPath();
                        if (split.getConnectorSplit().getLastModifiedTime() > indexLastModifiedTimeOfThisPartition) {
                            filteredSplits.add(split);
                        }
                        else if (lookUpResults.containsKey(filePathStr)) {
                            Pair<Long, Long> targetRange = new Pair<>(split.getConnectorSplit().getStartIndex(),
                                    split.getConnectorSplit().getEndIndex());

                            // do stripe matching: check if [targetStart, targetEnd] has any overlapping with the matching stripes
                            // first sort matching stripes, e.g. (5,10), (18,25), (30,35), (35, 40)
                            // then do binary search for both start and end of the target
                            List<Pair<Long, Long>> stripes = lookUpResults.get(filePathStr);
                            stripes.sort(Comparator.comparingLong(Pair::getFirst));
                            if (rangeSearch(stripes, targetRange)) {
                                filteredSplits.add(split);
                            }
                        }
                    }
                }
            }

            return filteredSplits;
        }
        catch (Throwable e) {
            LOG.debug("Exception occurred while filtering. Returning original splits", e);
            return inputSplits;
        }
    }

    /**
     * Get the name of the partition which holds this split.
     * <p>
     * If the table is not partitioned (no partition key found in path) then return the given string
     */
    private static String getPartitionKeyOrElse(String filePathStr, String defaultString)
    {
        Path filePath = Paths.get(filePathStr);
        String strInPartitionPlace = filePath.getName(filePath.getNameCount() - 2).toString();
        return strInPartitionPlace.contains("=") ? strInPartitionPlace : defaultString;
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
            FunctionHandle builtInFunctionHandle = call.getFunctionHandle();
            if (builtInFunctionHandle instanceof BuiltInFunctionHandle) {
                Signature signature = ((BuiltInFunctionHandle) builtInFunctionHandle).getSignature();
                if (signature.getName().getObjectName().equals("not")) {
                    return true;
                }
                try {
                    OperatorType operatorType = Signature.unmangleOperator(signature.getName().getObjectName());
                    if (operatorType.isComparisonOperator() && operatorType != IS_DISTINCT_FROM) {
                        return true;
                    }
                    return false;
                }
                catch (IllegalArgumentException e) {
                    return false;
                }
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
    public static Pair<Optional<RowExpression>, Map<Symbol, ColumnHandle>> getExpression(SqlStageExecution stage)
    {
        List<PlanNode> filterNodeOptional = getFilterNode(stage);

        if (filterNodeOptional.size() == 0) {
            return new Pair<>(Optional.empty(), new HashMap<>());
        }

        if (filterNodeOptional.get(0) instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) filterNodeOptional.get(0);
            if (filterNode.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                if (tableScanNode.getPredicate().isPresent()
                        && isSupportedExpression(tableScanNode.getPredicate().get())) { /* if total filter is not supported use the filterNode */
                    return new Pair<>(tableScanNode.getPredicate(), tableScanNode.getAssignments());
                }

                return new Pair<>(Optional.of(filterNode.getPredicate()), tableScanNode.getAssignments());
            }

            return new Pair<>(Optional.empty(), new HashMap<>());
        }

        if (filterNodeOptional.get(0) instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) filterNodeOptional.get(0);
            if (tableScanNode.getPredicate().isPresent()) {
                return new Pair<>(tableScanNode.getPredicate(), tableScanNode.getAssignments());
            }
        }

        return new Pair<>(Optional.empty(), new HashMap<>());
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
                FunctionHandle builtInFunctionHandle = call.getFunctionHandle();
                Signature signature;
                if (builtInFunctionHandle instanceof BuiltInFunctionHandle) {
                    signature = ((BuiltInFunctionHandle) builtInFunctionHandle).getSignature();
                }
                else {
                    return;
                }
                OperatorType operatorType = Signature.unmangleOperator(signature.getName().getObjectName());
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
        if (expression instanceof CallExpression) {
            FunctionHandle builtInFunctionHandle = ((CallExpression) expression).getFunctionHandle();
            Signature signature;
            if (builtInFunctionHandle instanceof BuiltInFunctionHandle) {
                signature = ((BuiltInFunctionHandle) builtInFunctionHandle).getSignature();
                if (signature.getName().getObjectName().contains("CAST")) {
                    // extract the inner expression for CAST expressions
                    return extractExpression(((CallExpression) expression).getArguments().get(0));
                }
            }
        }
        return expression;
    }

    /**
     * Search to find if a target range has any overlap with the given list of ranges
     *
     * @param ranges a list of sorted ranges (inclusive)
     * @param target a target range (inclusive)
     * @return if the target range has any overlap with the range list (inclusive)
     */
    public static boolean rangeSearch(List<Pair<Long, Long>> ranges, Pair<Long, Long> target)
    {
        int start = 0;
        int end = ranges.size() - 1;

        while (start + 1 < end) {
            int mid = start + end >>> 1;
            if (ranges.get(mid).getFirst().equals(target.getFirst())) {
                return true;
            }
            else if (ranges.get(mid).getFirst() < target.getFirst()) {
                start = mid;
            }
            else {
                end = mid;
            }
        }

        if (target.getFirst() < ranges.get(start).getFirst()) {
            return target.getSecond() >= ranges.get(start).getFirst() ||
                    (start > 0 && target.getFirst() <= ranges.get(start - 1).getSecond());
        }
        else {
            return target.getFirst() <= ranges.get(start).getSecond() ||
                    (start + 1 < ranges.size() && ranges.get(start + 1).getFirst() <= target.getSecond() && ranges.get(start + 1).getSecond() >= target.getFirst());
        }
    }
}
