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

package io.prestosql.spi.heuristicindex;

import io.prestosql.spi.connector.CreateIndexMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Interface implemented by all types of indexes (ie, Bloom, minmax etc)
 *
 * @since 2019-08-18
 */
public interface Index
        extends Closeable
{
    /**
     * The data unit on which the index should be applied on.
     * <p>
     * The index should be self-contained on the data unit specified and can work independently.
     *
     * @return data level on which the index should be applied on.
     */
    Set<CreateIndexMetadata.Level> getSupportedIndexLevels();

    /**
     * Gets the id of the IndexStore.
     * <p>
     * This value will be used to identify the index type of a stored index. e.g.
     * as the file extension of the index file.
     *
     * @return String id
     */
    String getId();

    default boolean supportMultiColumn()
    {
        return false;
    }

    /**
     * Adds the given values to the index.
     *
     * @param values a map of columnName-columnValues
     * @return whether the values are successfully added
     */
    boolean addValues(List<Pair<String, List<Object>>> values)
            throws IOException;

    /**
     * Add a list of value->symbol pairs to index, for example, 3 -> "ORCFile1"
     * <p>
     * If the list contains duplicate keys, the value of the first occurrence is used.
     *
     * @param pairs an ordered list of KeyValues to add to index, sorted ascending on Keys
     */
    default void addKeyValues(List<Pair<String, List<Pair<Comparable<? extends Comparable<?>>, String>>>> pairs)
    {
        throw new UnsupportedOperationException("This index does not support adding Key-Value pairs.");
    }

    /**
     * The Index will apply the provided Expression but only return a
     * boolean indicating whether the Expression matches any values in the index.
     *
     * @param expression the expression to apply
     * @return whether the expression result contains
     */
    boolean matches(Object expression) throws UnsupportedOperationException;

    /**
     * Given an Expression, the Index should apply it and return the matching positions.
     * For example given a > 5, a Bitmap index will return all positions that match.
     * <p>
     * Not all Index implementations will support this, for example a BloomIndex does not return positions,
     * instead it will implement the matches() method.
     *
     * @param expression the expression to apply
     * @return the Iterator of positions that matches the expression result
     * {@code null} if the index does not support lookUp operation
     */
    default <T extends Comparable<T>> Iterator<T> lookUp(Object expression)
            throws UnsupportedOperationException
    {
        return null;
    }

    /**
     * <pre>
     * Persist the index to the Outputstream.
     *
     * This method does not guarantee if the stream will be closed or left open.
     * The caller should ensure the stream is closed.
     * </pre>
     *
     * @param out OutputStream to write index to
     * @throws IOException In the case that an error with the filesystem occurs
     */
    void serialize(OutputStream out) throws IOException;

    /**
     * <pre>
     * Load the index from the provided InputStream.
     *
     * This method does not guarantee if the stream will be closed or left open.
     * The caller should ensure the stream is closed.
     * </pre>
     *
     * @param in InputStream to read index from
     * @throws IOException In the case that an error with the filesystem occurs
     */
    Index deserialize(InputStream in) throws IOException;

    /**
     * Intersect this index with another index and return the intersection index object.
     *
     * @param another another index to intersect with
     * @return the intersect index
     */
    default Index intersect(Index another) throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException(String.format("Intersect operation on %s index is not currently supported", getId()));
    }

    /**
     * Union this index with another index and return the union index object.
     *
     * @param another another index to union with
     * @return the union index
     */
    default Index union(Index another) throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException(String.format("Union operation on %s index is not currently supported", getId()));
    }

    /**
     * <pre>
     * Returns the properties of the Index.
     *
     * These properties may be used as configs for the Index. For example,
     * for a Bloom index, these properties could be used to set the FPP value.
     * Each Index determines how to use these properties.
     * </pre>
     *
     * @return Properties object.If not implemented returns an empty object
     */
    default Properties getProperties()
    {
        return new Properties();
    }

    /**
     * <pre>
     * Sets the properties of the Index.
     *
     * These properties may be used as configs for the Index. For example,
     * for a Bloom index, these properties could be used to set the FPP value.
     * Each Index determines how to use these properties.
     * </pre>
     *
     * @param properties Properties being set
     */
    default void setProperties(Properties properties)
    {
    }

    /**
     * Sets the expected number of entries that will be written to this index.
     * <p>
     * This value can help some index types to be better optimized.
     *
     * @return
     */
    default void setExpectedNumOfEntries(int expectedNumOfEntries)
    {
    }

    /**
     * <pre>
     * Returns the estimated memory consumed by this index.
     *
     * This memory usage can help with memory usage based cache eviction.
     *
     * The unit is Bytes.
     * </pre>
     *
     * @return long returns estimated bytes of memory consumed by this index
     */
    default long getMemoryUsage()
    {
        return 0;
    }

    /**
     * <pre>
     * Returns the estimated disk consumed by this index.
     *
     * This disk usage can help with disk usage based cache eviction.
     *
     * The unit is Bytes.
     * </pre>
     *
     * @return long returns estimated bytes of disk consumed by this index
     */
    default long getDiskUsage()
    {
        return 0;
    }

    default void close() throws IOException
    {
    }
}
