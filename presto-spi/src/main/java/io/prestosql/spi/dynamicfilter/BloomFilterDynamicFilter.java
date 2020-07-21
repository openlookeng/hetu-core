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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

public class BloomFilterDynamicFilter
        extends DynamicFilter
{
    public static final Logger log = Logger.get(BloomFilterDynamicFilter.class);

    private static final float DEFAULT_BLOOM_FILTER_FPP = 0.1F;

    private byte[] bloomFilterSerialized;
    private BloomFilter bloomFilterDeserialized;

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
        // TODO: Only support String value for now, fix this and use original value type
        return bloomFilterDeserialized.test(((String) value).getBytes());
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
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bloomFilterSerialized)) {
            bloomFilterDeserialized = BloomFilter.readFrom(bis);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to deserialize dynamic filter: " + columnHandle.toString());
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
        BloomFilter bloomFilter = new BloomFilter(DEFAULT_DYNAMIC_FILTER_SIZE, DEFAULT_BLOOM_FILTER_FPP);
        for (Object value : stringValueSet) {
            if (value instanceof Slice) {
                value = new String(((Slice) value).getBytes());
            }
            bloomFilter.add(String.valueOf(value).getBytes());
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
