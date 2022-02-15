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
package io.hetu.core.plugin.heuristicindex.index.bitmap;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.SerializerCompressionWrapper;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.extractType;
import static io.hetu.core.heuristicindex.util.IndexServiceUtils.getSerializer;
import static io.prestosql.spi.heuristicindex.TypeUtils.getActualValue;

/**
 * <pre>
 * Bitmap index based on Btree and RoaringBitmap.
 *
 * Usage:
 * 1. Create new instance of index: BitmapIndex idx = new BitmapIndex()
 * 2. Add values: idx.addValues()
 * 3. Persist index to file: idx.serialize()
 * 4. Close index: idx.close()
 * 5. Create new instance of index: BitmapIndex idx = new BitmapIndex()
 * 6. Load index from existing file: idx.deserialize()
 * 7. Query the index: idx.lookup()
 *
 * BitmapIndex#addValues only supports a single column, composite indexes are currently not supported.
 *
 * BitmapIndex#lookup and BitmapIndex#matches currently only support Domain object expressions with
 * equality only, e.g. column=value.
 *
 * </pre>
 */
public class BitmapIndex
        implements Index
{
    public static final String ID = "BITMAP";
    private static final Logger log = Logger.get(BitmapIndex.class);

    // configuration properties
    /**
     * increasing this value may result in smaller index size because there will be more data to compress per node
     * however, lookups will be slower because the entire node needs to be uncompressed to read values
     */
    protected static final String MAX_VALUES_PER_NODE_KEY = "bitmap.values_per_node";

    private static final int DEFAULT_MAX_VALUES_PER_NODE = 32;

    private static final String BTREE_MAP_ID = "MAP";
    private static final String BTREE_MAP_KEY_TYPE = "BTREE_KEY_TYPE";

    private Properties properties;
    private int maxValuesPerNode = DEFAULT_MAX_VALUES_PER_NODE;
    private DB db;
    private BTreeMap btree;
    private File file;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean updateAllowed = new AtomicBoolean(true);
    private final Map<Object, RoaringBitmap> cache = new HashMap<>();
    private long memoryUsage;

    @Override
    public Set<CreateIndexMetadata.Level> getSupportedIndexLevels()
    {
        return ImmutableSet.of(CreateIndexMetadata.Level.STRIPE);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public boolean addValues(List<Pair<String, List<Object>>> values)
            throws IOException
    {
        checkClosed();

        // values can only be added once
        if (!updateAllowed.getAndSet(false)) {
            throw new UnsupportedOperationException("Unable to update index. " +
                    "An existing Btree index can not be updated because all values must be added together since the " +
                    "position of the values is important.");
        }

        if (values.size() != 1) {
            throw new UnsupportedOperationException("Only single column is supported.");
        }

        List<Object> columnValues = values.get(0).getSecond();

        Map<Object, ArrayList<Integer>> positions = new HashMap<>();

        for (int i = 0; i < columnValues.size(); i++) {
            Object value = columnValues.get(i);
            if (value != null) {
                positions.computeIfAbsent(value, k -> new ArrayList<>()).add(i);
            }
        }

        if (positions.isEmpty()) {
            return true;
        }

        List<kotlin.Pair> bitmaps = new ArrayList<>(positions.size());
        for (Map.Entry<Object, ArrayList<Integer>> e : positions.entrySet()) {
            int[] valuePositions = ArrayUtils.toPrimitive(e.getValue().toArray(new Integer[0]));
            RoaringBitmap rr = RoaringBitmap.bitmapOf(valuePositions);
            rr.runOptimize();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            rr.serialize(dos);
            dos.close();
            Object value = convertToSupportedType(e.getKey());

            bitmaps.add(new kotlin.Pair(value, bos.toByteArray()));
        }
        Collections.sort(bitmaps, (o1, o2) -> ((Comparable) o1.component1()).compareTo(o2.component1()));
        getBtreeWriteOptimized(bitmaps.iterator().next().component1(), bitmaps.iterator());

        return true;
    }

    @Override
    public boolean matches(Object expression)
    {
        return lookUp(expression).hasNext();
    }

    /**
     * The lookup value is used to cache the created RoaringBitmap for
     * future queries
     * @param lookupValue
     * @param byteArray
     * @return
     */
    private RoaringBitmap byteArrayToBitmap(Object lookupValue, Object byteArray)
    {
        return cache.computeIfAbsent(lookupValue, k -> {
            if (byteArray == null) {
                return null;
            }
            byte[] value = (byte[]) byteArray;
            ByteBuffer bb = ByteBuffer.wrap(value);
            ImmutableRoaringBitmap bm = new ImmutableRoaringBitmap(bb);
            RoaringBitmap result = new RoaringBitmap(bm);
            memoryUsage += result.getSizeInBytes();
            return result;
        });
    }

    @Override
    public Iterator<Integer> lookUp(Object expression)
    {
        checkClosed();

        if (expression instanceof Domain) {
            Domain predicate = (Domain) expression;
            List<Range> ranges = ((SortedRangeSet) (predicate.getValues())).getOrderedRanges();

            try {
                ArrayList<RoaringBitmap> allMatches = new ArrayList<>();
                for (Range range : ranges) {
                    if (range.isSingleValue()) {
                        // unique value(for example: id=1, id in (1,2) (IN operator gives single exact values one by one)), bound: EXACTLY
                        Object value = getActualValue(predicate.getType(), range.getSingleValue());
                        Object byteArray = getBtreeReadOptimized().get(value);
                        if (byteArray != null) {
                            RoaringBitmap bitmap = byteArrayToBitmap(value, byteArray);
                            allMatches.add(bitmap);
                        }
                    }
                    else {
                        // <, <=, >=, >, BETWEEN
                        boolean highBoundless = range.getHigh().isUpperUnbounded();
                        boolean lowBoundless = range.getLow().isLowerUnbounded();
                        ConcurrentNavigableMap<Object, byte[]> concurrentNavigableMap = null;

                        if (highBoundless && !lowBoundless) {
                            Object low = getActualValue(predicate.getType(), range.getLow().getValue());
                            Object high = getBtreeReadOptimized().lastKey();
                            boolean fromInclusive = range.getLow().getBound().equals(Marker.Bound.EXACTLY);
                            if (getBtreeReadOptimized().comparator().compare(low, high) > 0) {
                                Object temp = low;
                                low = high;
                                high = temp;
                            }
                            concurrentNavigableMap = getBtreeReadOptimized().subMap(low, fromInclusive, high, true);
                        }
                        else if (!highBoundless && lowBoundless) {
                            // <= or <
                            Object low = getBtreeReadOptimized().firstKey();
                            Object high = getActualValue(predicate.getType(), range.getHigh().getValue());
                            boolean toInclusive = range.getHigh().getBound().equals(Marker.Bound.EXACTLY);
                            if (getBtreeReadOptimized().comparator().compare(low, high) > 0) {
                                Object temp = low;
                                low = high;
                                high = temp;
                            }
                            concurrentNavigableMap = getBtreeReadOptimized().subMap(low, true, high, toInclusive);
                        }
                        else if (!highBoundless && !lowBoundless) {
                            // BETWEEN
                            Object low = getActualValue(predicate.getType(), range.getLow().getValue());
                            Object high = getActualValue(predicate.getType(), range.getHigh().getValue());
                            if (getBtreeReadOptimized().comparator().compare(low, high) > 0) {
                                Object temp = low;
                                low = high;
                                high = temp;
                            }
                            concurrentNavigableMap = getBtreeReadOptimized().subMap(low, true, high, true);
                        }
                        else {
                            // This case, combined gives a range of boundless for both high and low end
                            throw new UnsupportedOperationException("No use for bitmap index as all values are matched due to no bounds.");
                        }

                        for (Map.Entry<Object, byte[]> e : concurrentNavigableMap.entrySet()) {
                            if (e != null) {
                                RoaringBitmap bitmap = byteArrayToBitmap(e.getKey(), e.getValue());
                                allMatches.add(bitmap);
                            }
                        }
                    }
                }

                if (allMatches.size() == 0) {
                    return Collections.emptyIterator();
                }

                if (allMatches.size() == 1) {
                    return allMatches.get(0).iterator();
                }

                return RoaringBitmap.or(allMatches.iterator()).iterator();
            }
            catch (Exception e) {
                throw new UnsupportedOperationException("Unsupported expression type.", e);
            }
        }
        else {
            throw new UnsupportedOperationException("Unsupported expression type.");
        }
    }

    @Override
    public void serialize(OutputStream out) throws IOException
    {
        checkClosed();

        // close db so all data is flushed to file
        getDbWriteOptimized().commit();
        getDbWriteOptimized().close();

        try (FileInputStream in = new FileInputStream(getFile());
                SnappyOutputStream sout = new SnappyOutputStream(out)) {
            IOUtils.copy(in, sout);
        }

        // set db to null; next operation will open the db up again
        db = null;
    }

    @Override
    public Index deserialize(InputStream in) throws IOException
    {
        checkClosed();

        try (FileOutputStream out = new FileOutputStream(getFile());
                SnappyInputStream sin = new SnappyInputStream(in)) {
            IOUtils.copy(sin, out);
        }

        // updating an existing bitmap is not allowed
        updateAllowed.set(false);

        return this;
    }

    @Override
    public Properties getProperties()
    {
        return properties;
    }

    @Override
    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    @Override
    public long getDiskUsage()
    {
        try {
            return getFile().length();
        }
        catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() throws IOException
    {
        if (db != null) {
            db.close();
        }

        if (!getFile().delete()) {
            log.debug("Failed to delete file: " + getFile().getName());
        }
        closed.set(true);
    }

    private DB getDbWriteOptimized() throws IOException
    {
        if (db == null) {
            db = DBMaker
                    .fileDB(getFile())
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .make();
            db.getStore().fileLoad();
        }

        return db;
    }

    private DB getDbReadOptimized() throws IOException
    {
        if (db == null) {
            db = DBMaker
                    .fileDB(getFile())
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .readOnly()
                    .make();
        }

        return db;
    }

    private BTreeMap getBtreeWriteOptimized(Object key, Iterator<kotlin.Pair> entries)
            throws IOException
    {
        if (btree == null) {
            // store the btree's key serializer type in the db, this will be required during reading
            // because when SerializerCompressionWrapper is used, the type must be explicitly known
            String serializerName = extractType(key);
            GroupSerializer keySerializer = getSerializer(serializerName);
            getDbWriteOptimized().atomicString(BTREE_MAP_KEY_TYPE, serializerName).create();

            btree = getDbWriteOptimized().treeMap(BTREE_MAP_ID)
                    .valuesOutsideNodesEnable()
                    .maxNodeSize(getMaxValuesPerNode())
                    .keySerializer(new SerializerCompressionWrapper(keySerializer))
                    .valueSerializer(Serializer.BYTE_ARRAY)
                    .createFrom(entries);
        }

        return btree;
    }

    private BTreeMap getBtreeReadOptimized() throws IOException
    {
        if (btree == null) {
            // read the explicit serializer type which was store earlier
            String serializerName = getDbReadOptimized().atomicString(BTREE_MAP_KEY_TYPE).open().get();

            if (serializerName == null) {
                throw new UnsupportedOperationException("Btree has not been initialized correctly.");
            }

            GroupSerializer keySerializer = getSerializer(serializerName);

            btree = getDbReadOptimized().treeMap(BTREE_MAP_ID)
                    .valuesOutsideNodesEnable()
                    .maxNodeSize(getMaxValuesPerNode())
                    .keySerializer(new SerializerCompressionWrapper(keySerializer))
                    .valueSerializer(Serializer.BYTE_ARRAY)
                    .open();
        }

        return btree;
    }

    private File getFile() throws IOException
    {
        if (file == null) {
            file = File.createTempFile("bitmapindex", UUID.randomUUID().toString());
            file.delete();
            file.deleteOnExit();
        }

        return file;
    }

    private int getMaxValuesPerNode()
    {
        if (getProperties() != null) {
            String maxNodesValue = getProperties().getProperty(MAX_VALUES_PER_NODE_KEY);
            maxValuesPerNode = maxNodesValue == null ? maxValuesPerNode : Integer.parseInt(maxNodesValue);
        }

        return maxValuesPerNode;
    }

    /**
     * Explicitly check type because it must be Comparable and there must be a corresponding Serializer available
     *
     * @param obj
     * @return
     */
    private Object convertToSupportedType(Object obj)
    {
        if ((obj instanceof Long)
                || (obj instanceof Double)
                || (obj instanceof Float)
                || (obj instanceof Date)
                || (obj instanceof Boolean)
                || (obj instanceof String)
                || (obj instanceof BigDecimal)) {
            return obj;
        }
        else if (obj instanceof Integer) {
            // convert int to long because when Expression or Domain is used while querying, they always use long
            return Long.valueOf((Integer) obj);
        }
        else {
            throw new UnsupportedOperationException("Unsupported value type. " +
                    "BitmapIndex does not support " + obj.getClass().toString());
        }
    }

    private void checkClosed()
    {
        if (closed.get()) {
            throw new UnsupportedOperationException("Index is closed.");
        }
    }
}
