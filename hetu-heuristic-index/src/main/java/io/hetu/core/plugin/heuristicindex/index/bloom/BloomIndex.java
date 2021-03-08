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

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.util.BloomFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.matchCallExpEqual;
import static io.prestosql.spi.heuristicindex.TypeUtils.getActualValue;

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

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public Set<CreateIndexMetadata.Level> getSupportedIndexLevels()
    {
        return ImmutableSet.of(CreateIndexMetadata.Level.STRIPE);
    }

    @Override
    public synchronized boolean addValues(List<Pair<String, List<Object>>> values)
    {
        // Currently expecting only one column
        List<Object> columnIdxValue = values.get(0).getSecond();
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
        // test Domain matching
        if (expression instanceof Domain) {
            Domain predicate = (Domain) expression;
            if (predicate.isSingleValue()) {
                Object value = getActualValue(predicate.getType(), predicate.getSingleValue());
                return getFilter().test(value.toString().getBytes());
            }
        }
        else if (expression instanceof CallExpression) {
            // test ComparisonExpression matching
            return matchCallExpEqual(expression, object -> filter.test(object.toString().getBytes()));
        }

        throw new UnsupportedOperationException("Expression not supported by " + ID + " index.");
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

    private int getExpectedNumOfEntries()
    {
        return expectedNumOfEntries;
    }

    @Override
    public void setExpectedNumOfEntries(int expectedNumOfEntries)
    {
        this.expectedNumOfEntries = expectedNumOfEntries;
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

    @Override
    public long getMemoryUsage()
    {
        return getFilter().getRetainedSizeInBytes();
    }
}
