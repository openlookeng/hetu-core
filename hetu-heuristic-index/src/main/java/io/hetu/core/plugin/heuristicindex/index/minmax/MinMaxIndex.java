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

package io.hetu.core.plugin.heuristicindex.index.minmax;

import com.google.common.collect.ImmutableSet;
import io.hetu.core.common.util.SecureObjectInputStream;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.relation.CallExpression;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.hetu.core.heuristicindex.util.IndexConstants.TYPES_WHITELIST;
import static io.prestosql.spi.heuristicindex.TypeUtils.extractValueFromRowExpression;

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
    public long getMemoryUsage()
    {
        if (min instanceof String) {
            return ((String) min).getBytes(StandardCharsets.UTF_8).length + ((String) max).getBytes(StandardCharsets.UTF_8).length;
        }
        else {
            return 0; // if the data type is not a string, the memory usage will be too small to converted to a positive number in KB
        }
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
        // Currently expecting only one column
        List<Object> columnIdxValue = values.get(0).getSecond();
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
    public Set<CreateIndexMetadata.Level> getSupportedIndexLevels()
    {
        return ImmutableSet.of(CreateIndexMetadata.Level.STRIPE);
    }

    @Override
    public boolean matches(Object expression)
    {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            BuiltInFunctionHandle builtInFunctionHandle;
            if (callExp.getFunctionHandle() instanceof BuiltInFunctionHandle) {
                builtInFunctionHandle = (BuiltInFunctionHandle) callExp.getFunctionHandle();
            }
            else {
                throw new UnsupportedOperationException("Unsupported function: " + callExp.getDisplayName());
            }
            Optional<OperatorType> operatorOptional = Signature.getOperatorType(builtInFunctionHandle.getSignature().getNameSuffix());
            if (operatorOptional.isPresent()) {
                OperatorType operator = operatorOptional.get();
                Comparable value = (Comparable) extractValueFromRowExpression(callExp.getArguments().get(1));
                switch (operator) {
                    case EQUAL:
                        return (value.compareTo(min) >= 0) && (value.compareTo(max) <= 0);
                    case LESS_THAN:
                        return value.compareTo(min) > 0;
                    case LESS_THAN_OR_EQUAL:
                        return value.compareTo(min) >= 0;
                    case GREATER_THAN:
                        return value.compareTo(max) < 0;
                    case GREATER_THAN_OR_EQUAL:
                        return value.compareTo(max) <= 0;
                    default:
                        throw new UnsupportedOperationException("Unsupported operator " + operator);
                }
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
