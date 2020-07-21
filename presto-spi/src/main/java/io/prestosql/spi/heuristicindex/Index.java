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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Interface implemented by all types of indexes (ie, Bloom, minmax etc)
 *
 * @param <T> Type to be indexed on
 * @since 2019-08-18
 */
public interface Index<T>
{
    /**
     * Gets the id of the IndexStore.
     * <p>
     * This value will be used to identify the index type of a stored index. e.g.
     * as the file extension of the index file.
     *
     * @return String id
     */
    String getId();

    /**
     * Adds the given values to the index.
     *
     * @param values values to add
     */
    void addValues(T[] values);

    /**
     * <pre>
     * If the provided value matches the operator for this Index.
     *
     * Each Index type will support different types of operations,
     * for example, a search index like Bloom will support the Equality (=) operator,
     * but another index like MinMax may support multiple operators
     * Greater than (>), Less than (<), etc.
     *
     * This method allows the Index implementation to decide how to check if the
     * index matches the provided value.
     *
     * Use Index#supports to check which operators are supported by the Index.
     * </pre>
     *
     * @param value    Thing being searched for in the index
     * @param operator Condition being applied in the search (ie, =, > , =>)
     * @return true if the value matches
     * the operator
     * @throws IllegalArgumentException if operator is not supported
     */
    boolean matches(T value, Operator operator) throws IllegalArgumentException;

    /**
     * Get an iterator over all values matching the filter
     *
     * @param filter the filter to apply
     * @param <I>    type of the list returned
     * @return list of type I
     */
    default <I> Iterator<I> getMatches(Object filter)
    {
        return null;
    }

    /**
     * Apply predicates on multiple index objects and intersect the result.
     * <p>
     * The current object should be included in the map if it should
     * have a filter applied on it.
     * <p>
     * The Index objects provided should all be the same type.
     *
     * @param indexToPredicate list of indexes and their predicates
     * @param <I>              type of list to return
     * @return list of type I
     */
    default <I> Iterator<I> getMatches(Map<Index, Object> indexToPredicate)
    {
        return null;
    }

    /**
     * Returns true if the index Supports the operator, false otherwise
     *
     * @param operator Enum storing the operator (ie, =,<,=<)
     * @return true if the Index supports the operator
     */
    boolean supports(Operator operator);

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
    void persist(OutputStream out) throws IOException;

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
    void load(InputStream in) throws IOException;

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
}
