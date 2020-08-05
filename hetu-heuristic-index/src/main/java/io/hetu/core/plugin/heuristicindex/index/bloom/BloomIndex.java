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

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.airlift.slice.Slice;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Operator;
import io.prestosql.spi.predicate.Domain;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toMap;

/**
 * Bloom index implementation
 *
 * @param <T> type to be indexed on
 */
public class BloomIndex<T>
        implements Index<T>
{
    public static final String ID = "BLOOM";
    private static final String FPP_KEY = "fpp";

    private static final double DEFAULT_FPP = 0.05;
    protected static final int DEFAULT_EXPECTED_NUM_OF_SIZE = 200000;

    private Properties properties;
    private BloomFilter<String> filter;
    private double fpp = DEFAULT_FPP;
    private int expectedNumOfEntries = DEFAULT_EXPECTED_NUM_OF_SIZE;

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public void addValues(T[] values)
    {
        for (T value : values) {
            if (value != null) {
                getFilter().put(value.toString());
            }
        }
    }

    /**
     * Perform search using the bloom filter but the returned results might be false-positive.
     *
     * @param value value to be searched in the bloom filter
     * @return true if index might contain the provided value
     */
    public boolean mightContain(T value)
    {
        return matches(value, Operator.EQUAL);
    }

    @Override
    public boolean matches(T value, Operator operator)
            throws IllegalArgumentException
    {
        if (operator == null) {
            throw new IllegalArgumentException("No operator provided.");
        }

        if (operator == Operator.EQUAL) {
            return getFilter().mightContain(value.toString());
        }
        else {
            throw new IllegalArgumentException("Unsupported operator " + operator);
        }
    }

    @Override
    public <I> Iterator<I> getMatches(Object filter)
    {
        Map<Index, Object> self = new HashMap<>();
        self.put(this, filter);

        return getMatches(self);
    }

    @Override
    public <I> Iterator<I> getMatches(Map<Index, Object> indexToPredicate)
    {
        Map<BloomIndex, Domain> bloomIndexDomainMap = indexToPredicate.entrySet().stream()
                .collect(toMap(e -> (BloomIndex) e.getKey(), e -> (Domain) e.getValue()));

        boolean flag = true;

        for (Map.Entry<BloomIndex, Domain> entry : bloomIndexDomainMap.entrySet()) {
            BloomIndex bloomIndex = entry.getKey();
            Domain predicate = entry.getValue();

            if (predicate.isSingleValue()) {
                Class<?> javaType = predicate.getValues().getType().getJavaType();
                flag = flag && bloomIndex.mightContain(rangeValueToString(predicate.getSingleValue(), javaType));
            }
            else { // unsupported operator, not skip stripes
                flag = true;
            }
        }
        final boolean hasNext = flag;

        return (Iterator<I>) new Iterator<Integer>()
        {
            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public Integer next()
            {
                return null;
            }
        };
    }

    @Override
    public boolean supports(Operator operator)
    {
        return operator == Operator.EQUAL;
    }

    @Override
    public void persist(OutputStream out)
            throws IOException
    {
        getFilter().writeTo(out);
    }

    @Override
    public void load(InputStream in)
            throws IOException
    {
        filter = BloomFilter.readFrom(in, Funnels.stringFunnel(Charset.defaultCharset()));
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
    public void setExpectedNumOfEntries(int expectedNumOfEntries)
    {
        this.expectedNumOfEntries = expectedNumOfEntries;
    }

    @Override
    public int getExpectedNumOfEntries()
    {
        return expectedNumOfEntries;
    }

    private double getFpp()
    {
        if (getProperties() != null) {
            String fppValue = getProperties().getProperty(FPP_KEY);
            fpp = fppValue == null ? fpp : Double.parseDouble(fppValue);
        }

        return fpp;
    }

    private BloomFilter<String> getFilter()
    {
        if (filter == null) {
            filter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), getExpectedNumOfEntries(), getFpp());
        }

        return filter;
    }

    /**
     * <pre>
     *  get range value, if it is slice, we should change it to string
     * </pre>
     * @param object value
     * @param javaType value java type
     * @return string
     */
    private String rangeValueToString(Object object, Class<?> javaType)
    {
        return javaType == Slice.class ? ((Slice) object).toStringUtf8() : object.toString();
    }
}
