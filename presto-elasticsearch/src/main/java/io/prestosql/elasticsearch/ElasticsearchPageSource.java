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
package io.prestosql.elasticsearch;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.elasticsearch.decoders.ArrayDecoder;
import io.prestosql.elasticsearch.decoders.BigintDecoder;
import io.prestosql.elasticsearch.decoders.BooleanDecoder;
import io.prestosql.elasticsearch.decoders.Decoder;
import io.prestosql.elasticsearch.decoders.DoubleDecoder;
import io.prestosql.elasticsearch.decoders.IdColumnDecoder;
import io.prestosql.elasticsearch.decoders.IntegerDecoder;
import io.prestosql.elasticsearch.decoders.IpAddressDecoder;
import io.prestosql.elasticsearch.decoders.RealDecoder;
import io.prestosql.elasticsearch.decoders.RowDecoder;
import io.prestosql.elasticsearch.decoders.ScoreColumnDecoder;
import io.prestosql.elasticsearch.decoders.SmallintDecoder;
import io.prestosql.elasticsearch.decoders.SourceColumnDecoder;
import io.prestosql.elasticsearch.decoders.TimestampDecoder;
import io.prestosql.elasticsearch.decoders.TinyintDecoder;
import io.prestosql.elasticsearch.decoders.VarbinaryDecoder;
import io.prestosql.elasticsearch.decoders.VarcharDecoder;
import io.prestosql.elasticsearch.optimization.ElasticAggregationBuilder;
import io.prestosql.elasticsearch.optimization.ElasticsearchAggregationsCompositeResult;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.elasticsearch.BuiltinColumns.ID;
import static io.prestosql.elasticsearch.BuiltinColumns.SCORE;
import static io.prestosql.elasticsearch.BuiltinColumns.SOURCE;
import static io.prestosql.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.IPADDRESS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

public class ElasticsearchPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(ElasticsearchPageSource.class);

    private final List<Decoder> decoders;

    private final ElasticSearchResultIterator iterator;
    private final BlockBuilder[] columnBuilders;
    private final List<ElasticsearchColumnHandle> columns;
    private long totalBytes;
    private long readTimeNanos;
    private boolean finished;

    public ElasticsearchPageSource(
            ElasticsearchClient client,
            ElasticsearchTableHandle table,
            ElasticsearchSplit split,
            List<ElasticsearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(columns, "columns is null");

        this.columns = ImmutableList.copyOf(columns);

        decoders = createDecoders(columns);

        // When the _source field is requested, we need to bypass column pruning when fetching the document
        boolean needAllFields = columns.stream()
                .map(ElasticsearchColumnHandle::getName)
                .anyMatch(isEqual(SOURCE.getName()));

        // Columns to fetch as doc_fields instead of pulling them out of the JSON source
        // This is convenient for types such as DATE, TIMESTAMP, etc, which have multiple possible
        // representations in JSON, but a single normalized representation as doc_field.
        List<String> documentFields = flattenFields(columns).entrySet().stream()
                .filter(entry -> entry.getValue().equals(TIMESTAMP))
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        columnBuilders = columns.stream()
                .map(ElasticsearchColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        List<String> requiredFields = columns.stream()
                .map(ElasticsearchColumnHandle::getName)
                .filter(name -> !BuiltinColumns.NAMES.contains(name))
                .collect(toList());

        long start = System.nanoTime();
        SearchResponse searchResponse = client.beginSearch(
                table.getIndex(),
                split.getShard(),
                buildSearchQuery(table.getConstraint(), columns, table.getQuery()),
                needAllFields ? Optional.empty() : Optional.of(requiredFields),
                documentFields, table.getElasticAggOptimizationContext());
        readTimeNanos += System.nanoTime() - start;
        this.iterator = ElasticsearchResultIteratorFactory.getIterator(client, searchResponse);
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + iterator.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished || !iterator.hasNext();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        iterator.close();
    }

    @Override
    public Page getNextPage()
    {
        if (columnBuilders.length == 0) {
            // TODO: emit "count" query against Elasticsearch
            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }

            finished = true;
            return new Page(count);
        }

        long size = 0;
        while (size < PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES && iterator.hasNext()) {
            populateResultSet();

            size = Arrays.stream(columnBuilders)
                    .mapToLong(BlockBuilder::getSizeInBytes)
                    .sum();
        }

        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }

        return new Page(blocks);
    }

    private void populateResultSet()
    {
        if (iterator instanceof SearchHitIterator) {
            SearchHit hit = (SearchHit) iterator.next();
            Map<String, Object> document = hit.getSourceAsMap();

            for (int i = 0; i < decoders.size(); i++) {
                String field = columns.get(i).getName();
                decoders.get(i).decode(hit, () -> getField(document, field), columnBuilders[i]);
            }

            if (hit.getSourceRef() != null) {
                totalBytes += hit.getSourceRef().length();
            }
        }
        else if (iterator instanceof AggregationIterator) {
            ElasticsearchAggregationsCompositeResult elasticsearchAggregationsCompositeResult = (ElasticsearchAggregationsCompositeResult) iterator.next();
            if (elasticsearchAggregationsCompositeResult.getAggregations() != null) {
                Map<String, Aggregation> document = elasticsearchAggregationsCompositeResult.getAggregations().asMap();

                for (int i = 0; i < decoders.size(); i++) {
                    String field = columns.get(i).getName();
                    decoders.get(i).decode(elasticsearchAggregationsCompositeResult.getAggregations(), () -> getAggregationOrField(document, field, elasticsearchAggregationsCompositeResult.getGroupbyKeyValueMap()), columnBuilders[i]);
                }

                // not calculable directly
                totalBytes += 0;
            }
        }
    }

    public static Object getField(Map<String, Object> document, String field)
    {
        Object value = document.get(field);
        if (value == null) {
            Map<String, Object> result = new HashMap<>();
            String prefix = field + ".";
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(prefix)) {
                    result.put(key.substring(prefix.length()), entry.getValue());
                }
            }

            if (!result.isEmpty()) {
                return result;
            }
        }

        return value;
    }

    public static Object getAggregationOrField(Map<String, Aggregation> document, String field, Map<String, Object> groupByFieldValueMap)
    {
        String escapedFieldName = ElasticAggregationBuilder.getEscapedFieldName(field);
        Object value = null;
        if (groupByFieldValueMap != null) {
            value = groupByFieldValueMap.get(escapedFieldName);
        }
        Object result = null;
        int resultCount = 0;
        if (value == null) {
            for (Map.Entry<String, Aggregation> entry : document.entrySet()) {
                String key = entry.getKey();
                if (key.equalsIgnoreCase(escapedFieldName)) {
                    result = getUnderlyingAggregationValue(entry.getValue());
                    ++resultCount;
                }
            }

            if (resultCount == 1) {
                return result;
            }
        }

        return value;
    }

    private static Object getUnderlyingAggregationValue(Aggregation value)
    {
        if (value instanceof NumericMetricsAggregation.SingleValue) {
            NumericMetricsAggregation.SingleValue valueCount = (NumericMetricsAggregation.SingleValue) value;
            return valueCount.value();
        }
        // unsupported optimization and hence shouldn't reach below
        return value;
    }

    private Map<String, Type> flattenFields(List<ElasticsearchColumnHandle> columns)
    {
        Map<String, Type> result = new HashMap<>();

        for (ElasticsearchColumnHandle column : columns) {
            flattenFields(result, column.getName(), column.getType());
        }

        return result;
    }

    private void flattenFields(Map<String, Type> result, String fieldName, Type type)
    {
        if (type instanceof RowType) {
            for (RowType.Field field : ((RowType) type).getFields()) {
                flattenFields(result, appendPath(fieldName, field.getName().get()), field.getType());
            }
        }
        else {
            result.put(fieldName, type);
        }
    }

    private List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> {
                    if (column.getName().equals(ID.getName())) {
                        return new IdColumnDecoder();
                    }

                    if (column.getName().equals(SCORE.getName())) {
                        return new ScoreColumnDecoder();
                    }

                    if (column.getName().equals(SOURCE.getName())) {
                        return new SourceColumnDecoder();
                    }

                    return createDecoder(column.getName(), column.getType());
                })
                .collect(toImmutableList());
    }

    private Decoder createDecoder(String path, Type type)
    {
        if (type.equals(VARCHAR)) {
            return new VarcharDecoder(path);
        }
        else if (type.equals(VARBINARY)) {
            return new VarbinaryDecoder(path);
        }
        else if (type.equals(TIMESTAMP)) {
            return new TimestampDecoder(path);
        }
        else if (type.equals(BOOLEAN)) {
            return new BooleanDecoder(path);
        }
        else if (type.equals(DOUBLE)) {
            return new DoubleDecoder(path);
        }
        else if (type.equals(REAL)) {
            return new RealDecoder(path);
        }
        else if (type.equals(TINYINT)) {
            return new TinyintDecoder(path);
        }
        else if (type.equals(SMALLINT)) {
            return new SmallintDecoder(path);
        }
        else if (type.equals(INTEGER)) {
            return new IntegerDecoder(path);
        }
        else if (type.equals(BIGINT)) {
            return new BigintDecoder(path);
        }
        else if (type.getTypeSignature().getBase().equals(IPADDRESS)) {
            return new IpAddressDecoder(path, type);
        }
        else if (type instanceof RowType) {
            RowType rowType = (RowType) type;

            List<Decoder> decoderList = rowType.getFields().stream()
                    .map(field -> createDecoder(appendPath(path, field.getName().get()), field.getType()))
                    .collect(toImmutableList());

            List<String> fieldNames = rowType.getFields().stream()
                    .map(RowType.Field::getName)
                    .map(Optional::get)
                    .collect(toImmutableList());

            return new RowDecoder(path, fieldNames, decoderList);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return new ArrayDecoder(createDecoder(path, elementType));
        }

        throw new UnsupportedOperationException("Type not supported: " + type);
    }

    private static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }

    private static class SearchHitIterator
            extends ElasticSearchResultIterator<SearchHit>
    {
        private SearchHits searchHits;

        public SearchHitIterator(ElasticsearchClient client, Supplier<SearchResponse> first)
        {
            super(client, first);
        }

        @Override
        protected SearchHit computeNext()
        {
            if (scrollId == null) {
                long start = System.nanoTime();
                SearchResponse response = first.get();
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }
            else if (currentPosition == searchHits.getHits().length) {
                long start = System.nanoTime();
                SearchResponse response = client.nextPage(scrollId);
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }

            if (currentPosition == searchHits.getHits().length) {
                return endOfData();
            }

            SearchHit hit = searchHits.getAt(currentPosition);
            currentPosition++;

            return hit;
        }

        private void reset(SearchResponse response)
        {
            scrollId = response.getScrollId();
            searchHits = response.getHits();
            currentPosition = 0;
        }
    }

    private abstract static class ElasticSearchResultIterator<T>
            extends AbstractIterator<T>
    {
        protected final ElasticsearchClient client;
        protected final Supplier<SearchResponse> first;
        protected String scrollId;

        protected int currentPosition;
        protected long readTimeNanos;

        protected ElasticSearchResultIterator(ElasticsearchClient client, Supplier<SearchResponse> first)
        {
            this.client = client;
            this.first = first;
        }

        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        public void close()
        {
            if (scrollId != null) {
                try {
                    client.clearScroll(scrollId);
                }
                catch (Exception e) {
                    // ignore
                    LOG.debug("Error clearing scroll", e);
                }
            }
        }
    }

    private static class AggregationIterator
            extends ElasticSearchResultIterator<ElasticsearchAggregationsCompositeResult>
    {
        private List<Aggregations> aggregationsList;

        private List<Map<String, Object>> keyList;

        public AggregationIterator(ElasticsearchClient client, Supplier<SearchResponse> first)
        {
            super(client, first);
        }

        @Override
        protected ElasticsearchAggregationsCompositeResult computeNext()
        {
            if (scrollId == null) {
                long start = System.nanoTime();
                SearchResponse response = first.get();
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }
            else if (currentPosition == aggregationsList.size()) {
                long start = System.nanoTime();
                SearchResponse response = client.nextPage(scrollId);
                readTimeNanos += System.nanoTime() - start;
                reset(response);
                return endOfData();
            }
            Aggregations aggregations = aggregationsList.get(currentPosition);

            Map<String, Object> key = null;
            if (keyList != null && keyList.size() > currentPosition) {
                key = keyList.get(currentPosition);
            }
            currentPosition++;

            return new ElasticsearchAggregationsCompositeResult(aggregations, key);
        }

        private void reset(SearchResponse response)
        {
            scrollId = response.getScrollId();
            aggregationsList = new ArrayList<>();
            keyList = new ArrayList<>();
            if (isGroupByClausePresent(Collections.singletonList(response.getAggregations()))) {
                Map<String, Object> groupingKeyValueMap = new HashMap<>();

                flattenGroupBy(Collections.singletonList(response.getAggregations()), groupingKeyValueMap);
            }
            else {
                if (response.getAggregations() != null && !response.getAggregations().asList().isEmpty()) {
                    this.aggregationsList = Collections.singletonList(response.getAggregations());
                }
            }

            currentPosition = 0;
        }

        private void flattenGroupBy(List<Aggregations> aggregationsList, Map<String, Object> groupingKeyValueMap)
        {
            if (isGroupByClausePresent(aggregationsList)) {
                Aggregation aggregation = aggregationsList.get(0).asList().get(0);
                String groupingKey = aggregation.getName().split("group_by_clause_")[1];
                ParsedTerms parsedTerms = (ParsedTerms) aggregation;
                List<? extends Terms.Bucket> parsedTermsBuckets = parsedTerms.getBuckets();
                for (Terms.Bucket parsedTermsBucket : parsedTermsBuckets) {
                    Aggregations aggregations = parsedTermsBucket.getAggregations();
                    if (isGroupByClausePresent(Collections.singletonList(aggregations))) {
                        Object keyValue = parsedTermsBucket.getKey();
                        groupingKeyValueMap.put(groupingKey, keyValue);
                        flattenGroupBy(Collections.singletonList(aggregations), groupingKeyValueMap);
                    }
                    else {
                        Object keyValue = parsedTermsBucket.getKey();
                        groupingKeyValueMap.put(groupingKey, keyValue);
                        keyList.add(new HashMap<>(groupingKeyValueMap));
                        this.aggregationsList.add(aggregations);
                    }
                }
            }
        }

        private boolean isGroupByClausePresent(List<Aggregations> aggregationsList)
        {
            return aggregationsList != null && !aggregationsList.isEmpty() && aggregationsList.get(0) != null && !aggregationsList.get(0).asList().isEmpty() && aggregationsList.get(0).asList().get(0).getName().startsWith("group_by_clause_");
        }
    }

    private static class ElasticsearchResultIteratorFactory
    {
        public static ElasticSearchResultIterator getIterator(ElasticsearchClient client, SearchResponse searchResponse)
        {
            ElasticSearchResultIterator elasticSearchResultIterator;
            if (searchResponse.getAggregations() != null) {
                elasticSearchResultIterator = new AggregationIterator(client, () -> searchResponse);
            }
            else {
                elasticSearchResultIterator = new SearchHitIterator(client, () -> searchResponse);
            }

            return elasticSearchResultIterator;
        }
    }
}
