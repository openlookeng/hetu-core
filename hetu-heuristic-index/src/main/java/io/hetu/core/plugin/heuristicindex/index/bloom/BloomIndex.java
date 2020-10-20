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

package io.hetu.core.plugin.heuristicindex.index.bloom;

import io.airlift.slice.Slice;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.tree.ComparisonExpression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.hetu.core.heuristicindex.util.TypeUtils.extractSingleValue;

/**
 * Bloom index implementation
 */
public class BloomIndex
        implements Index
{
    public static final String ID = "BLOOM";
    protected static final int DEFAULT_EXPECTED_NUM_OF_SIZE = 200000;
    private static final String FPP_KEY = "bloom.fpp";
    private static final double DEFAULT_FPP = 0.001;
    private Properties properties;
    private BloomFilter filter;
    private double fpp = DEFAULT_FPP;
    private int expectedNumOfEntries = DEFAULT_EXPECTED_NUM_OF_SIZE;
    private long memorySize;

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public boolean addValues(Map<String, List<Object>> values)
    {
        // Currently expecting only one column
        List<Object> columnIdxValue = values.values().iterator().next();
        for (Object value : columnIdxValue) {
            if (value != null) {
                getFilter().add(value.toString().getBytes());
            }
        }
        return true;
    }

    @Override
    public synchronized boolean matches(Object expression)
    {
        if (expression instanceof Domain) {
            Domain predicate = (Domain) expression;
            if (predicate.isSingleValue()) {
                Class<?> javaType = predicate.getValues().getType().getJavaType();
                return getFilter().test(rangeValueToString(predicate.getSingleValue(), javaType).getBytes());
            }
            else {
                // Does not support multiple predicate for now. Do not filter.
                return true;
            }
        }

        if (expression instanceof ComparisonExpression) {
            ComparisonExpression compExp = (ComparisonExpression) expression;
            ComparisonExpression.Operator operator = compExp.getOperator();
            Object value = extractSingleValue(compExp.getRight());

            if (operator == ComparisonExpression.Operator.EQUAL) {
                return getFilter().test(value.toString().getBytes());
            }

            throw new IllegalArgumentException("Unsupported operator " + operator);
        }

        // Not supported expression. Do not filter.
        return true;
    }

    @Override
    public void serialize(OutputStream out)
            throws IOException
    {
        getFilter().writeTo(out);
    }

    @Override
    public Index deserialize(InputStream in)
            throws IOException
    {
        filter = BloomFilter.readFrom(in);
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
    public boolean supportMultiColumn()
    {
        return false;
    }

    private double getFpp()
    {
        if (getProperties() != null) {
            String fppValue = getProperties().getProperty(FPP_KEY);
            fpp = fppValue == null ? fpp : Double.parseDouble(fppValue);
        }

        return fpp;
    }

    private BloomFilter getFilter()
    {
        if (filter == null) {
            filter = new BloomFilter(getExpectedNumOfEntries(), getFpp());
        }

        return filter;
    }

    /**
     * <pre>
     *  get range value, if it is slice, we should change it to string
     * </pre>
     *
     * @param object value
     * @param javaType value java type
     * @return string
     */
    private String rangeValueToString(Object object, Class<?> javaType)
    {
        return javaType == Slice.class ? ((Slice) object).toStringUtf8() : object.toString();
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
