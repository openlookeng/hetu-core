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

package io.hetu.core.plugin.heuristicindex.index.bloom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.matchCallExpEqual;
import static io.prestosql.spi.heuristicindex.TypeUtils.getActualValue;

/**
 * Bloom index implementation
 */
public class BloomIndex
        implements Index
{
    public static final String ID = "BLOOM";
    private Properties properties;
    private BloomFilter filter;
    private static final Logger log = Logger.get(BloomIndex.class);
    private int expectedNumOfEntries;

    private static final String FPP_KEY = "bloom.fpp";
    private static final double DEFAULT_FPP = 0.001;
    private double fpp = DEFAULT_FPP;

    private static final String MMAP_KEY = "bloom.mmapEnabled";
    private static final Boolean DEFAULT_MMAP = true;
    private Boolean mmap;

    private File file;
    private int mmapSizeInByte;

    @Override
    public String getId()
    {
        return ID;
    }

    @VisibleForTesting
    void setMmapEnabled(Boolean mmap)
    {
        // use mmap file to hold the bloom filter if enabled
        this.mmap = mmap;
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
        HashSet<Object> columnIdxValue = new HashSet<>(values.get(0).getSecond());
        for (Object value : columnIdxValue) {
            if (value != null) {
                getFilterFromMemory().add(value.toString().getBytes(StandardCharsets.UTF_8));
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
                return getFilter().test(value.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        else if (expression instanceof CallExpression) {
            // test ComparisonExpression matching
            return matchCallExpEqual(expression, object -> getFilter().test(object.toString().getBytes(StandardCharsets.UTF_8)));
        }
        throw new UnsupportedOperationException("Expression not supported by " + ID + " index.");
    }

    private void writeToMmap(BloomFilter curFilter)
            throws IOException
    {
        try (RandomAccessFile randomFile = new RandomAccessFile(getFile(), "rw")) {
            try (FileChannel channel = randomFile.getChannel()) {
                long[] bits = curFilter.getBitSet();
                int numHashFunctions = curFilter.getNumHashFunctions();
                int numBits = bits.length;
                mmapSizeInByte = numBits * 8;
                MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, 2 * 4 + mmapSizeInByte);
                map.putInt(numHashFunctions);
                map.putInt(numBits);
                for (int i = 0; i < numBits; i++) {
                    map.putLong(bits[i]);
                }
            }
        }
    }

    private BloomFilter readFromMmap()
            throws IOException
    {
        try (RandomAccessFile randomFile = new RandomAccessFile(getFile(), "r")) {
            try (FileChannel channel = randomFile.getChannel()) {
                MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, 2 * 4 + mmapSizeInByte);
                int numHashFunctions = map.getInt();
                int numBits = map.getInt();
                long[] bits = new long[numBits];
                for (int i = 0; i < numBits; i++) {
                    bits[i] = map.getLong();
                }
                return new BloomFilter(bits, numHashFunctions);
            }
        }
    }

    @Override
    public void serialize(OutputStream out)
            throws IOException
    {
        getFilterFromMemory().writeTo(out);
    }

    @Override
    public Index deserialize(InputStream in)
            throws IOException
    {
        if (isMmapEnabled()) {
            // write to mmap and do not write memory
            writeToMmap(BloomFilter.readFrom(in));
        }
        else {
            // deserialize filter to memory
            filter = BloomFilter.readFrom(in);
        }
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
        if (expectedNumOfEntries < 1) {
            throw new IllegalArgumentException("Expected number of entries must be greater than 0");
        }
        return expectedNumOfEntries;
    }

    private File getFile() throws IOException
    {
        if (file == null) {
            file = File.createTempFile("bloomindex", UUID.randomUUID().toString());
            if (!file.delete()) {
                log.error("could not delete file");
            }
            file.deleteOnExit();
        }
        return file;
    }

    @Override
    public void close() throws IOException
    {
        if (isMmapEnabled()) {
            if (!getFile().delete()) {
                log.error("could not delete file");
            }
        }
    }

    @Override
    public void setExpectedNumOfEntries(int expectedNumOfEntries)
    {
        this.expectedNumOfEntries = expectedNumOfEntries;
        if (expectedNumOfEntries < 1) {
            throw new IllegalArgumentException("Expected number of entries must be greater than 0");
        }
    }

    private double getFpp()
    {
        if (getProperties() != null) {
            String fppValue = getProperties().getProperty(FPP_KEY);
            fpp = fppValue == null ? fpp : Double.parseDouble(fppValue);
        }
        return fpp;
    }

    private boolean isMmapEnabled()
    {
        if (mmap == null) {
            if (getProperties() != null) {
                String mmapValue = getProperties().getProperty(MMAP_KEY);
                mmap = mmapValue == null ? DEFAULT_MMAP : Boolean.parseBoolean(mmapValue);
            }
            else {
                mmap = DEFAULT_MMAP;
            }
        }
        return mmap;
    }

    private BloomFilter getFilterFromMemory()
    {
        if (filter == null) {
            filter = new BloomFilter(getExpectedNumOfEntries(), getFpp());
        }
        return filter;
    }

    @VisibleForTesting
    BloomFilter getFilter()
    {
        if (isMmapEnabled()) {
            try {
                return readFromMmap();
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Error reading bloom filter from mmap", e);
            }
        }
        else {
            return getFilterFromMemory();
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return filter == null ? 0 : filter.getRetainedSizeInBytes();
    }

    @Override
    public long getDiskUsage()
    {
        return mmap ? 2 * 4 + mmapSizeInByte : 0;
    }
}
