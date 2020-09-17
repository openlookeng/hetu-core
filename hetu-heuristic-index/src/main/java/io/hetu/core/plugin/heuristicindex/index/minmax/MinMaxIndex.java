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

import io.hetu.core.common.util.SecureObjectInputStream;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Operator;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Objects;

import static io.hetu.core.heuristicindex.util.IndexConstants.TYPES_WHITELIST;

/**
 * MinMax index implementation. It can be used to check whether a value is in or out of the given range.
 *
 * @param <T> Type to be created index for
 */
public class MinMaxIndex<T>
        implements Index<T>
{
    public static final String ID = "MINMAX";

    private Comparable min;
    private Comparable max;
    private long memorySize;

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
    public MinMaxIndex(T min, T max)
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
    public void addValues(T[] values)
    {
        for (T v : values) {
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
    }

    @Override
    public boolean matches(T value, Operator operator) throws IllegalArgumentException
    {
        if (operator == null) {
            throw new IllegalArgumentException("No operator provided.");
        }

        Comparable v = (Comparable) value;

        switch (operator) {
            case EQUAL:
                return (v.compareTo(min) > 0 || v.compareTo(min) == 0)
                        && (v.compareTo(max) < 0 || v.compareTo(max) == 0);
            case LESS_THAN:
                return v.compareTo(min) > 0;
            case LESS_THAN_OR_EQUAL:
                return v.compareTo(min) > 0 || v.compareTo(min) == 0;
            case GREATER_THAN:
                return v.compareTo(max) < 0;
            case GREATER_THAN_OR_EQUAL:
                return v.compareTo(max) < 0 || v.compareTo(max) == 0;
            default:
                throw new IllegalArgumentException("Unsupported operator " + operator);
        }
    }

    @Override
    public boolean supports(Operator operator)
    {
        // If the min and max values are not set (usually this happens when user somehow created an empty index),
        // then we should return false to disable the current index
        if (min == null || max == null) {
            return false;
        }
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

    /**
     * Returns whether the provided value is within the min max values.
     *
     * @param v Value to be checked whether it is contained in this MinMax index.
     * @return True if min <= v <= max
     */
    public boolean contains(T v)
    {
        return matches(v, Operator.EQUAL);
    }

    /**
     * Matches Operator.GREATER_THAN
     *
     * @param v Value to be checked whether it is greater than the maximum value
     * @return True if v > max
     */
    public boolean greaterThan(T v)
    {
        return matches(v, Operator.GREATER_THAN);
    }

    /**
     * Matches Operator.GREATER_THAN_OR_EQUAL
     *
     * @param v Value to be checked whether it is greater than or equal to the maximum value
     * @return true if v >= max
     */
    public boolean greaterThanEqual(T v)
    {
        return matches(v, Operator.GREATER_THAN_OR_EQUAL);
    }

    /**
     * Matches Operator.LESS_THAN
     *
     * @param v Value to be checked whether it is less than the maximum value
     * @return true if v < min
     */
    public boolean lessThan(T v)
    {
        return matches(v, Operator.LESS_THAN);
    }

    /**
     * Matches Operator.LESS_THAN_OR_EQUAL
     *
     * @param v Value to be checked whether it is less than or equal to the maximum value
     * @return true if v <= min
     */
    public boolean lessThanEqual(T v)
    {
        return matches(v, Operator.LESS_THAN_OR_EQUAL);
    }

    @Override
    public void persist(OutputStream out) throws IOException
    {
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(min);
        oos.writeObject(max);
    }

    @Override
    public void load(InputStream in) throws IOException
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
    }

    @Override
    public void setExpectedNumOfEntries(int expectedNumOfEntries)
    {
        // ignore
    }

    @Override
    public int getExpectedNumOfEntries()
    {
        return 0;
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
        MinMaxIndex<?> that = (MinMaxIndex<?>) o;
        return min.equals(that.min)
                && max.equals(that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max);
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
