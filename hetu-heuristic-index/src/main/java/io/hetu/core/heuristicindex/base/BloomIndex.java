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

package io.hetu.core.heuristicindex.base;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.Operator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Properties;

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
    private static final String EXPECTED_SIZE_KEY = "size";

    private static final double DEFAULT_FPP = 0.05;
    private static final int DEFAULT_EXPECTED_SIZE = 200000;

    private Properties properties;
    private BloomFilter<String> filter;
    private double fpp = DEFAULT_FPP;
    private int expectedSize = DEFAULT_EXPECTED_SIZE;

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
    public boolean matches(T value, Operator operator) throws IllegalArgumentException
    {
        if (operator == null) {
            throw new IllegalArgumentException(String.format("No operator provided."));
        }

        if (operator == Operator.EQUAL) {
            return getFilter().mightContain(value.toString());
        }
        else {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Unsupported operator %s.", operator));
        }
    }

    @Override
    public boolean supports(Operator operator)
    {
        return operator == Operator.EQUAL;
    }

    @Override
    public void persist(OutputStream out) throws IOException
    {
        getFilter().writeTo(out);
    }

    @Override
    public void load(InputStream in) throws IOException
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

    private double getFpp()
    {
        if (getProperties() != null) {
            String fppValue = getProperties().getProperty(FPP_KEY);
            fpp = fppValue == null ? fpp : Double.valueOf(fppValue);
        }

        return fpp;
    }

    private int getExpectedSize()
    {
        if (getProperties() != null) {
            String expectedSizeValue = getProperties().getProperty(EXPECTED_SIZE_KEY);
            expectedSize = expectedSizeValue == null ? expectedSize : Integer.parseInt(expectedSizeValue);
        }

        return expectedSize;
    }

    private BloomFilter<String> getFilter()
    {
        if (filter == null) {
            filter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), getExpectedSize(), getFpp());
        }

        return filter;
    }
}
