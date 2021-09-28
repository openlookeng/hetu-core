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
package io.prestosql.spi.dynamicfilter;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BloomFilterDynamicFilter
        extends DynamicFilter
{
    public static final Logger log = Logger.get(BloomFilterDynamicFilter.class);
    public static final int DEFAULT_DYNAMIC_FILTER_SIZE = 1024 * 1024;
    public static final double DEFAULT_BLOOM_FILTER_FPP = 0.1D;
    private byte[] bloomFilterSerialized;
    private BloomFilter bloomFilterDeserialized;

    public BloomFilterDynamicFilter(String filterId, ColumnHandle columnHandle, byte[] bloomFilterSerialized, Type type)
    {
        this.filterId = filterId;
        this.columnHandle = columnHandle;
        this.bloomFilterSerialized = bloomFilterSerialized;
        this.type = type;
        bloomFilterDeserialized = deserializeBloomFilter(bloomFilterSerialized);
    }

    public BloomFilterDynamicFilter(String filterId, ColumnHandle columnHandle, BloomFilter bloomFilterDeserialized, Type type)
    {
        this.filterId = filterId;
        this.type = type;
        this.columnHandle = columnHandle;
        this.bloomFilterDeserialized = bloomFilterDeserialized;
    }

    private BloomFilterDynamicFilter(String filterId, ColumnHandle columnHandle, BloomFilter bloomFilterDeserialized, byte[] bloomFilterSerialized, Type type)
    {
        this.filterId = filterId;
        this.type = type;
        this.columnHandle = columnHandle;
        this.bloomFilterSerialized = bloomFilterSerialized;
        this.bloomFilterDeserialized = bloomFilterDeserialized;
    }

    public static BloomFilterDynamicFilter fromHashSetDynamicFilter(HashSetDynamicFilter hashSetDynamicFilter)
    {
        return fromHashSetDynamicFilter(hashSetDynamicFilter, DEFAULT_BLOOM_FILTER_FPP);
    }

    public static BloomFilterDynamicFilter fromHashSetDynamicFilter(HashSetDynamicFilter hashSetDynamicFilter, double bloomFilterFpp)
    {
        BloomFilter bloomFilter = BloomFilterDynamicFilter.createBloomFilterFromSet(hashSetDynamicFilter.getSetValues(), bloomFilterFpp);
        return new BloomFilterDynamicFilter(hashSetDynamicFilter.getFilterId(), hashSetDynamicFilter.getColumnHandle(), bloomFilter, hashSetDynamicFilter.getType());
    }

    public static BloomFilter createBloomFilterFromSet(Set valueSet, double bloomFilterFpp)
    {
        BloomFilter bloomFilter = new BloomFilter(DEFAULT_DYNAMIC_FILTER_SIZE, bloomFilterFpp);
        for (Object value : valueSet) {
            if (value instanceof Long) {
                bloomFilter.addLong((Long) value);
            }
            else if (value instanceof Slice) {
                bloomFilter.add((Slice) value);
            }
            else {
                bloomFilter.add(String.valueOf(value).getBytes());
            }
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

    public static BloomFilterDynamicFilter fromCombinedDynamicFilter(CombinedDynamicFilter df)
    {
        List<DynamicFilter> filters = df.getFilters();

        List<BloomFilterDynamicFilter> bloomFilter = filters.stream().filter(f -> f instanceof BloomFilterDynamicFilter)
                .map(BloomFilterDynamicFilter.class::cast)
                .collect(Collectors.toList());
        if (bloomFilter.size() > 0) {
            return bloomFilter.get(0);
        }

        List<HashSetDynamicFilter> hashSetDynamicFilters = filters.stream().filter(f -> f instanceof HashSetDynamicFilter)
                .map(HashSetDynamicFilter.class::cast)
                .collect(Collectors.toList());
        if (hashSetDynamicFilters.size() > 0) {
            return fromHashSetDynamicFilter(hashSetDynamicFilters.get(0));
        }

        return null;
    }

    @Override
    public boolean contains(Object value)
    {
        // TODO: Only support Long and Slice value for now, fix this and use original value type
        if (value == null) {
            return bloomFilterDeserialized.test((byte[]) null);
        }

        if (value instanceof Long) {
            return bloomFilterDeserialized.test((Long) value);
        }

        if (value instanceof Slice) {
            return bloomFilterDeserialized.test((Slice) value);
        }

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
        DynamicFilter clone = new BloomFilterDynamicFilter(filterId, columnHandle, bloomFilterDeserialized, bloomFilterSerialized, type);
        clone.setMax(max);
        clone.setMax(min);
        return clone;
    }

    @Override
    public boolean isEmpty()
    {
        return bloomFilterDeserialized.isEmpty();
    }

    private BloomFilter deserializeBloomFilter(byte[] bloomFilterSerialized)
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bloomFilterSerialized)) {
            return BloomFilter.readFrom(bis);
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

    public byte[] createSerializedBloomFilter()
    {
        this.bloomFilterSerialized = convertBloomFilterToByteArray(this.bloomFilterDeserialized);
        return this.bloomFilterSerialized;
    }

    public BloomFilter getBloomFilterDeserialized()
    {
        return bloomFilterDeserialized;
    }
}
