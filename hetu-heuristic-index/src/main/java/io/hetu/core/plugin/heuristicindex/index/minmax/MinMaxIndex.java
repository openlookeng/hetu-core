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

package io.hetu.core.plugin.heuristicindex.index.minmax;

import com.google.common.collect.ImmutableSet;
import io.hetu.core.common.util.SecureObjectInputStream;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.sql.tree.ComparisonExpression;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.hetu.core.heuristicindex.util.IndexConstants.TYPES_WHITELIST;
import static io.hetu.core.heuristicindex.util.TypeUtils.extractSingleValue;

/**
 * MinMax index implementation. It can be used to check whether a value is in or out of the given range.
 */
public class MinMaxIndex
        implements Index
{
    public static final String ID = "MINMAX";

    private Comparable min;
    private Comparable max;

    /**
     * Default Constructor
     */
    public MinMaxIndex()
    {
    }

    /**
     * Constructor
     *
     * @param min Minimum value of range
     * @param max Maximum value of range
     */
    public MinMaxIndex(Object min, Object max)
    {
        this.min = (Comparable) min;
        this.max = (Comparable) max;
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public Set<Level> getSupportedIndexLevels()
    {
        return ImmutableSet.of(Level.STRIPE);
    }

    @Override
    public synchronized boolean addValues(Map<String, List<Object>> values)
    {
        // Currently expecting only one column
        List<Object> columnIdxValue = values.values().iterator().next();
        for (Object v : columnIdxValue) {
            if (v == null) {
                continue;
            }

            Comparable value = (Comparable) v;
            if (min == null && max == null) {
                min = value;
                max = value;
            }
            else {
                if (value.compareTo(min) < 0) {
                    min = value;
                }

                if (value.compareTo(max) > 0) {
                    max = value;
                }
            }
        }
        return true;
    }

    @Override
    public boolean matches(Object expression)
    {
        if (expression instanceof ComparisonExpression) {
            ComparisonExpression compExp = (ComparisonExpression) expression;
            ComparisonExpression.Operator operator = compExp.getOperator();
            Comparable value = (Comparable) extractSingleValue(compExp.getRight());
            switch (operator) {
                case EQUAL:
                    return (value.compareTo(min) > 0 || value.compareTo(min) == 0)
                            && (value.compareTo(max) < 0 || value.compareTo(max) == 0);
                case LESS_THAN:
                    return value.compareTo(min) > 0;
                case LESS_THAN_OR_EQUAL:
                    return value.compareTo(min) > 0 || value.compareTo(min) == 0;
                case GREATER_THAN:
                    return value.compareTo(max) < 0;
                case GREATER_THAN_OR_EQUAL:
                    return value.compareTo(max) < 0 || value.compareTo(max) == 0;
                default:
                    throw new IllegalArgumentException("Unsupported operator " + operator);
            }
        }

        // Not supported expression. Don't filter out
        return true;
    }

    @Override
    public void serialize(OutputStream out)
            throws IOException
    {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(min);
        oos.writeObject(max);
    }

    @Override
    public Index deserialize(InputStream in)
            throws IOException
    {
        try (ObjectInputStream ois = new SecureObjectInputStream(in, TYPES_WHITELIST)) {
            // read min value
            Object obj = ois.readObject();
            if (obj instanceof Comparable) {
                this.min = (Comparable) obj;
            }
            else {
                throw new IOException("Invalid min value");
            }

            // read max value
            obj = ois.readObject();
            if (obj instanceof Comparable) {
                this.max = (Comparable) obj;
            }
            else {
                throw new IOException("Invalid max value");
            }
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }

        return this;
    }

    @Override
    public Index intersect(Index another)
    {
        if (!(another instanceof MinMaxIndex)) {
            throw new UnsupportedOperationException("MinMax Index cannot intersect with " + another.getClass().getCanonicalName());
        }

        MinMaxIndex theOther = (MinMaxIndex) another;
        Comparable newMin = this.min.compareTo(theOther.min) < 0 ? theOther.min : this.min;
        Comparable newMax = this.max.compareTo(theOther.max) < 0 ? this.max : theOther.max;

        return new MinMaxIndex(newMin, newMax);
    }

    @Override
    public Index union(Index another)
    {
        if (!(another instanceof MinMaxIndex)) {
            throw new UnsupportedOperationException("MinMax Index cannot union with " + another.getClass().getCanonicalName());
        }

        MinMaxIndex theOther = (MinMaxIndex) another;
        Comparable newMin = this.min.compareTo(theOther.min) > 0 ? theOther.min : this.min;
        Comparable newMax = this.max.compareTo(theOther.max) > 0 ? this.max : theOther.max;

        return new MinMaxIndex(newMin, newMax);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MinMaxIndex that = (MinMaxIndex) o;
        return min.equals(that.min)
                && max.equals(that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max);
    }
}
