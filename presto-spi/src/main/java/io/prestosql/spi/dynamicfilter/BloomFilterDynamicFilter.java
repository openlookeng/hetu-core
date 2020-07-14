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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;

public class BloomFilterDynamicFilter
        extends DynamicFilter
{
    public static final Logger log = Logger.get(BloomFilterDynamicFilter.class);

    private byte[] bloomFilterSerialized;
    private BloomFilter bloomFilterDeserialized;

    public static final double BLOOMFILTER_CREAETIONFPP = 0.1;
    public static final int DEFAULT_DYNAMIC_FILTER_SIZE = 1024 * 1024;

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

    public static BloomFilterDynamicFilter fromHashSetDynamicFilter(HashSetDynamicFilter hashSetDynamicFilter)
    {
        BloomFilter bloomFilter = BloomFilterDynamicFilter.createBloomFilterFromSet(hashSetDynamicFilter.getSetValues());
        return new BloomFilterDynamicFilter(hashSetDynamicFilter.getFilterId(), hashSetDynamicFilter.getColumnHandle(), bloomFilter, hashSetDynamicFilter.getType());
    }

    public byte[] createSerializedBloomFilter()
    {
        this.bloomFilterSerialized = convertBloomFilterToByteArray(this.bloomFilterDeserialized);
        return this.bloomFilterSerialized;
    }

    public static BloomFilter createBloomFilterFromSet(Set stringValueSet)
    {
        BloomFilter bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), DEFAULT_DYNAMIC_FILTER_SIZE, BLOOMFILTER_CREAETIONFPP);
        for (Object value : stringValueSet) {
            if (value instanceof Slice) {
                value = new String(((Slice) value).getBytes());
            }
            bloomFilter.put(String.valueOf(value));
        }
        return bloomFilter;
    }

    public static byte[] convertBloomFilterToByteArray(BloomFilter bloomFilter)
    {
        byte[] finalOutput = null;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            bloomFilter.writeTo(out);
            finalOutput = out.toByteArray();
        }
        catch (IOException e) {
            log.error("could not  finish filter, Exception happened:" + e.getMessage());
        }
        return finalOutput;
    }
}
