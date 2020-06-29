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
package io.prestosql.spi.dynamicfilter;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.prestosql.spi.connector.ColumnHandle;

import java.io.IOException;
import java.nio.charset.Charset;

public class BloomFilterDynamicFilter
        extends DynamicFilter
{
    private byte[] bloomFilterSerialized;
    private BloomFilter bloomFilterDeserialized;

    public BloomFilterDynamicFilter(String filterId, ColumnHandle columnHandle, byte[] bloomFilterSerialized, Type type)
    {
        this.filterId = filterId;
        this.columnHandle = columnHandle;
        this.bloomFilterSerialized = bloomFilterSerialized;
        this.type = type;
        createDeserializedBloomFilter();
    }

    public BloomFilterDynamicFilter(String filterId, ColumnHandle columnHandle, BloomFilter bloomFilterDeserialized, Type type)
    {
        this.filterId = filterId;
        this.type = type;
        this.columnHandle = columnHandle;
        this.bloomFilterDeserialized = bloomFilterDeserialized;
    }

    @Override
    public boolean contains(Object value)
    {
        return bloomFilterDeserialized.mightContain(value);
    }

    @Override
    public long getSize()
    {
        return bloomFilterDeserialized.approximateElementCount();
    }

    @Override
    public DynamicFilter clone()
    {
        DynamicFilter clone = new BloomFilterDynamicFilter(filterId, columnHandle, bloomFilterDeserialized, type);
        clone.setMax(max);
        clone.setMax(min);
        return clone;
    }

    public void createDeserializedBloomFilter()
    {
        try (java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(this.getBloomFilterSerialized())) {
            bloomFilterDeserialized = BloomFilter.readFrom(bis, Funnels.stringFunnel(Charset.defaultCharset()));
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to deserialize dynamic filter: " + this.getColumnHandle().toString());
        }
    }

    /**
     * Get serialized bloom filter of current dynamic filter
     *
     * @return serialized bloom filter in byte array, or null if not set
     */
    public byte[] getBloomFilterSerialized()
    {
        return bloomFilterSerialized;
    }

    public BloomFilter getBloomFilterDeserialized()
    {
        return bloomFilterDeserialized;
    }
}
