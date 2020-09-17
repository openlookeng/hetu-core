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

package io.hetu.core.plugin.heuristicindex.index.bitmap;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Operator;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnSelectorBitmapIndexSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.roaringbitmap.IntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * This BitMapIndexV2 will create and apply the filter for only one column data
 * Therefore, one druid segment class should be enough to hold the data.
 * <p>
 * The soomsh file can support upto 2GB,
 * however, for Druid to operate well under heavy query load, it is important
 * for the segment file size to be within the recommended range of 300mb-700mb.
 * (https://druid.apache.org/docs/latest/design/segments.html)
 * If your segment files are larger than this range, then consider either
 * changing the granularity of the time interval or partitioning your data
 * and tweaking the targetPartitionSize in your partitionsSpec (a good
 * starting point for this parameter is 5 million rows).
 */
public class BitMapIndex<T>
        implements Index<T>
{
    static final int DEFAULT_EXPECTED_NUM_OF_SIZE = 200000;
    private static final Logger LOG = LoggerFactory.getLogger(BitMapIndex.class);
    private static final String COLUMN_NAME = "column";
    private static final String ID = "BITMAP";
    private static final Charset CHARSET = StandardCharsets.ISO_8859_1;
    private static final char HEADER_TERMINATOR = '#';
    private static final String HEADER_FILE_INFO_SEPARATOR = ",";
    private static final String HEADER_FILE_INFO_PROPERTY_SEPARATOR = ":";
    private static final String SETTINGS = " {\n" +
            " \t\"bitmap\": {\n" +
            " \t\t\"type\": \"roaring\"\n" +
            " \t},\n" +
            " \t\"dimensionCompression\": \"lz4\",\n" +
            " \t\"metricCompression\": \"lz4\",\n" +
            " \t\"longEncoding\": \"auto\"\n" +
            " }";

    // when the index is initialized an IncrementalIndex is created
    // and values can be added to it
    // this IncrementalIndex can then be persisted to disk.
    // until the index is persisted to disk, it only provides write operation
    // it cannot be queried!
    // i.e. only values can be added to it, to query the index, it must
    // be written to disk then read so a QueryableIndex can be obtained
    //
    // when the persisted index is loaded from disk a QueryableIndex
    // is returned, this only provides the ability to read and query the index
    // i.e.an existing index can't be modified
    // if new values need to be added, a new index must be created
    // via IncrementalIndex and all values must be added again
    // therefore the flow should be:
    // 1. initialize IncrementalIndex
    // 2. add values
    // 3. write IncrementalIndex to disk
    // 4. load index from disk and get QueryableIndex
    // 5. query the QueryableIndex
    private IncrementalIndex incrementalIndex;
    private QueryableIndex queryableIndex;

    private AtomicLong rows = new AtomicLong();
    private int expectedNumOfEntries = DEFAULT_EXPECTED_NUM_OF_SIZE;
    private long memorySize;

    public BitMapIndex()
    {
    }

    /**
     * Loads index that was previously persisted to this dir.
     * The return QueryableIndex can then be queried.
     *
     * @param indexDir
     * @return
     * @throws IOException
     */
    private static QueryableIndex load(File indexDir) throws IOException
    {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        InjectableValues.Std injectableValues = new InjectableValues.Std();
        injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());
        jsonMapper.setInjectableValues(injectableValues);

        IndexIO indexIo = new IndexIO(jsonMapper, () -> 0);

        QueryableIndex queryIndex = indexIo.loadIndex(indexDir);

        return queryIndex;
    }

    /**
     * Persist the given IncrementalIndex to disk, it can then be loaded
     * to get a QueryableIndex
     *
     * @param incrementalIndex
     * @param outputDir
     * @throws IOException
     */
    private static void persist(IncrementalIndex incrementalIndex, File outputDir) throws IOException
    {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        InjectableValues.Std injectableValues = new InjectableValues.Std();
        injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());
        jsonMapper.setInjectableValues(injectableValues);

        IndexIO indexIo = new IndexIO(jsonMapper, () -> 0);
        IndexMergerV9 indexMergerV9 = new IndexMergerV9(jsonMapper, indexIo, OffHeapMemorySegmentWriteOutMediumFactory.instance());
        ObjectMapper objectMapper = new ObjectMapper();
        IndexSpec indexSpec = objectMapper.readValue(SETTINGS, IndexSpec.class);

        indexMergerV9.persist(incrementalIndex,
                outputDir,
                indexSpec,
                null);
    }

    /**
     * Given a Domain, extract the DimFilters
     * <p>
     * Supported operators: IN, Between, >=, >, <=, <, !=, Not IN
     *
     * @param predicate
     * @return
     */
    private static List<DimFilter> predicateToFilter(Domain predicate)
    {
        List<String> in = new ArrayList<>();
        List<DimFilter> orFilters = new ArrayList<>();
        List<DimFilter> filterList = new ArrayList<>();
        DimFilter newFilter;
        String lower;
        String upper;
        Boolean lowerStrict;
        Boolean upperStrict;
        List<Range> ranges = ((SortedRangeSet) (predicate.getValues())).getOrderedRanges();
        Class<?> javaType = predicate.getValues().getType().getJavaType();

        StringComparator comparator = (javaType == long.class || javaType == double.class) ?
                new StringComparators.AlphanumericComparator() :
                new StringComparators.LexicographicComparator();

        for (Range range : ranges) {
            // unique value(for example: id=1, id in (1,2)), bound: EXACTLY
            if (range.isSingleValue()) {
                String dimensionValue;
                dimensionValue = String.valueOf(rangeValueToString(range.getSingleValue(), javaType));
                in.add(dimensionValue);
            }
            // with upper/lower value, bound: ABOVE/BELOW
            else {
                lower = (range.getLow().getValueBlock().isPresent()) ?
                        String.valueOf(rangeValueToString(range.getLow().getValue(), javaType)) : null;
                upper = (range.getHigh().getValueBlock().isPresent()) ?
                        String.valueOf(rangeValueToString(range.getHigh().getValue(), javaType)) : null;
                lowerStrict = (lower != null) ? range.getLow().getBound() == Marker.Bound.ABOVE : null;
                upperStrict = (upper != null) ? range.getHigh().getBound() == Marker.Bound.BELOW : null;

                // dimension is not null(druid is not support int is not null, return all)
                newFilter = (lower == null && upper == null) ?
                        new NotDimFilter(new SelectorDimFilter(COLUMN_NAME, null, null)) :
                        new BoundDimFilter(COLUMN_NAME, lower, upper, lowerStrict, upperStrict, null, null, comparator);

                filterList.add(newFilter);
                // NOT IN (3,4), there are three boundDimFilters ("id < 3", "3 < id < 4", "id > 4")
                // != 3, three are two boundDimFilters ("id < 3", "id > 3")
                if (newFilter instanceof BoundDimFilter) {
                    orFilters.add(newFilter);
                }
            }
        }

        // operate is IN / =
        if (in.size() != 0) {
            newFilter = new InDimFilter(COLUMN_NAME, in, null);
            filterList.add(newFilter);
        }

        // NOT IN (3,4) become "id < 3" or "3 < id < 4" or "id > 4"
        // != 3 become "id < 3" or "id > 3"
        if (orFilters.size() > 1) {
            filterList.removeAll(orFilters);
            filterList.add(new OrDimFilter(orFilters));
        }

        // operate IS NULL (druid is not support int is null)
        if (ranges.isEmpty() && javaType == Slice.class) {
            newFilter = new SelectorDimFilter(COLUMN_NAME, null, null);
            filterList.add(newFilter);
        }

        return filterList;
    }

    /**
     * <pre>
     *  get range value, if it is slice, we should change it to string
     * </pre>
     *
     * @param object   value
     * @param javaType value java type
     * @return string
     */
    private static String rangeValueToString(Object object, Class<?> javaType)
    {
        return javaType == Slice.class ? ((Slice) object).toStringUtf8() : object.toString();
    }

    @Override
    public <I> Iterator<I> getMatches(Object filter)
    {
        Map<Index, Object> self = new HashMap<>();
        self.put(this, filter);

        return getMatches(self);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    /**
     * input as one column values
     *
     * @param values values to add
     */
    @Override
    public void addValues(T[] values)
    {
        for (T value : values) {
            Map<String, Object> events = new LinkedHashMap<>();
            events.put(COLUMN_NAME, value);
            MapBasedInputRow mapBasedInputRow = new MapBasedInputRow(rows.get(), Collections.singletonList(COLUMN_NAME), events);
            try {
                getOrInitIncrementalIndex().add(mapBasedInputRow);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            rows.incrementAndGet();
        }
    }

    /**
     * Bitmap requires a Domain as the value, the Domain will already include what kind
     * of operation is being performed, therefore the operator parameter is not required.
     *
     * @param operator not required since value should be a Domain
     * @return
     */
    @Override
    public boolean matches(Object value, Operator operator)
            throws IllegalArgumentException
    {
        if (!(value instanceof Domain)) {
            throw new IllegalArgumentException("Value must be a Domain.");
        }

        Iterator iterator = getMatches(value);

        if (iterator == null) {
            throw new IllegalArgumentException("Operation not supported.");
        }

        return iterator.hasNext();
    }

    @Override
    public boolean supports(Operator operator)
    {
        switch (operator) {
            case EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return true;
            default:
                return false;
        }
    }

    @Override
    public void persist(OutputStream out) throws IOException
    {
        File tmpDir = new File(FileUtils.getTempDirectory(), "bitmapsIndexTmp_" + UUID.randomUUID());
        try {
            File indexOutDir = new File(tmpDir, "indexOutDir");
            if (!indexOutDir.exists()) {
                indexOutDir.mkdirs();
            }
            // write index files to directory
            persist(getOrInitIncrementalIndex(), indexOutDir);

            // convert the files in the directory into a single stream
            combineDirIntoStream(out, indexOutDir);
        }
        finally {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    @Override
    public void load(InputStream in) throws IOException
    {
        File tmpDir = new File(FileUtils.getTempDirectory(), "bitmapsIndexTmp_" + UUID.randomUUID());
        try {
            File indexExtractDir = new File(tmpDir, "indexExtractDir");
            if (!indexExtractDir.exists()) {
                indexExtractDir.mkdirs();
            }
            extractInputStreamToDir(in, indexExtractDir);
            queryableIndex = load(indexExtractDir);
        }
        finally {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    @Override
    public int getExpectedNumOfEntries()
    {
        return expectedNumOfEntries;
    }

    @Override
    public void setExpectedNumOfEntries(int expectedNumOfEntries)
    {
        this.expectedNumOfEntries = expectedNumOfEntries;
    }

    @Override
    public <I> Iterator<I> getMatches(Map<Index, Object> indexToPredicate)
    {
        // indexToPredicate contains a mapping from BitMapIndex to predicates
        // it will do an intersection on the results of applying the predicate using the
        // corresponding index
        // the map should also include this current object itself
        // technically this is more like a utility method that should be static in the interface
        // however, each index implementation may have a more efficient way of
        // intersecting results, that's why each Index implementation will implement this method
        Map<BitMapIndex, Domain> bitMapIndexDomainMap = indexToPredicate.entrySet().stream()
                .collect(toMap(e -> (BitMapIndex) e.getKey(), e -> (Domain) e.getValue()));

        ImmutableBitmap lastBm = null;
        for (Map.Entry<BitMapIndex, Domain> entry : bitMapIndexDomainMap.entrySet()) {
            BitMapIndex bitMapIndex = entry.getKey();
            Domain predicate = entry.getValue();
            List<DimFilter> dimFilters = predicateToFilter(predicate);

            List<Filter> filters = dimFilters.stream()
                    .map(filter -> filter.toFilter()).collect(toList());

            QueryableIndex queryableIndex = bitMapIndex.getQueryableIndex();

            // if queryableIndex is null, it means the index has not been loaded from disk
            // so it cannot be queried
            if (queryableIndex == null) {
                continue;
            }

            ColumnSelectorBitmapIndexSelector bitmapIndexSelector = new ColumnSelectorBitmapIndexSelector(
                    queryableIndex.getBitmapFactoryForDimensions(),
                    VirtualColumns.nullToEmpty(null),
                    queryableIndex);
            BitmapResultFactory<?> bitmapResultFactory = new DefaultBitmapResultFactory(bitmapIndexSelector.getBitmapFactory());

            if (filters.size() == 0) {
                filters.add(new TrueFilter());
            }

            ImmutableBitmap bm = AndFilter.getBitmapIndex(bitmapIndexSelector, bitmapResultFactory, filters);

            if (lastBm == null) {
                lastBm = bm;
            }
            else {
                lastBm = lastBm.intersection(bm);
            }
        }

        if (lastBm == null) {
            return Collections.emptyIterator();
        }

        IntIterator intIterator = lastBm.iterator();

        return (Iterator<I>) new Iterator<Integer>()
        {
            @Override
            public boolean hasNext()
            {
                return intIterator.hasNext();
            }

            @Override
            public Integer next()
            {
                return intIterator.next();
            }
        };
    }

    /**
     * Reads the files from the provided dir and writes them to the output stream
     * <p>
     * A header is added at the start to provide file info
     * <p>
     * file1Name:file1Length,file2Name:file2Length#
     * file1Content
     * file2Content
     *
     * @param outputStream
     * @param dir
     * @throws IOException
     */
    private void combineDirIntoStream(OutputStream outputStream, File dir) throws IOException
    {
        File[] files = dir.listFiles();

        // first create the header line
        // this will contain the names and sizes for all the files being written to the stream
        StringBuilder header = new StringBuilder();
        for (int i = 0; files != null && i < files.length; i++) {
            File file = files[i];
            header.append(file.getName())
                    .append(HEADER_FILE_INFO_PROPERTY_SEPARATOR)
                    .append(file.length());

            if (i + 1 < files.length) {
                header.append(HEADER_FILE_INFO_SEPARATOR);
            }
        }
        header.append(HEADER_TERMINATOR);

        // write the header then all the file contents in order
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, CHARSET))) {
            writer.write(header.toString());

            for (File f : files) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), CHARSET))) {
                    IOUtils.copy(reader, writer);
                }
            }
        }
    }

    /**
     * Corresponding method to combineDirIntoStream()
     * <p>
     * Extracts the files from the stream and write them to the output dir
     *
     * @param inputStream
     * @param outputDir
     * @throws IOException
     */
    private void extractInputStreamToDir(InputStream inputStream, File outputDir)
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, CHARSET));

        // read the header
        StringBuilder header = new StringBuilder();
        int c = reader.read();
        while (c != -1 && (char) c != HEADER_TERMINATOR) {
            header.append((char) c);
            c = reader.read();
        }

        // read each file from the stream and write it out
        String[] fileInfos = header.toString().split(HEADER_FILE_INFO_SEPARATOR);
        for (int i = 0; i < fileInfos.length; i++) {
            String[] properties = fileInfos[i].split(HEADER_FILE_INFO_PROPERTY_SEPARATOR);
            int len = Integer.parseInt(properties[1]);
            File file = new File(outputDir, properties[0]);

            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), CHARSET))) {
                for (int j = 0; j < len; j++) {
                    writer.write(reader.read());
                }
            }
        }
    }

    /**
     * Gets or initializes an IncrementalIndex to which values to should added
     * Note: the IncrementalIndex can't be queried until it is first persisted
     * to disk
     *
     * @return
     */
    private IncrementalIndex getOrInitIncrementalIndex()
    {
        if (incrementalIndex == null) {
            incrementalIndex = new IncrementalIndex.Builder()
                    .setIndexSchema(
                            new IncrementalIndexSchema.Builder()
                                    .withTimestampSpec(new TimestampSpec("timestamp", "iso", null))
                                    .withQueryGranularity(Granularities.NONE)
                                    .withDimensionsSpec(new DimensionsSpec(Collections.singletonList(new StringDimensionSchema(COLUMN_NAME))))
                                    .withRollup(false)
                                    .build())
                    .setMaxRowCount(getExpectedNumOfEntries())
                    .buildOnheap();
        }

        return incrementalIndex;
    }

    QueryableIndex getQueryableIndex()
    {
        return this.queryableIndex;
    }

    @Override
    public long getMemorySize()
    {
        return this.memorySize;
    }

    @Override
    public void setMemorySize(long memorySize)
    {
        this.memorySize = memorySize;
    }
}
