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
package io.hetu.core.plugin.hbase.client;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * TestHBaseRegionLocator
 *
 * @since 2020-03-20
 */
public class TestHBaseRegionLocator
        implements RegionLocator
{
    @Override
    public HRegionLocation getRegionLocation(byte[] row)
            throws IOException
    {
        HRegionLocation getRegionLocation = null;
        return Optional.ofNullable(getRegionLocation).orElse(getRegionLocation);
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] row, boolean reload)
            throws IOException
    {
        HRegionLocation getRegionLocation = null;
        return Optional.ofNullable(getRegionLocation).orElse(getRegionLocation);
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] bytes, int i, boolean b) throws IOException
    {
        return null;
    }

    @Override
    public List<HRegionLocation> getRegionLocations(byte[] bytes, boolean b) throws IOException
    {
        return null;
    }

    @Override
    public void clearRegionLocationCache()
    {
    }

    @Override
    public List<HRegionLocation> getAllRegionLocations()
            throws IOException
    {
        List<HRegionLocation> hrls = null;
        return Optional.ofNullable(hrls).orElse(hrls);
    }

    @Override
    public byte[][] getStartKeys()
            throws IOException
    {
        byte[][] getStartKeys = null;
        return Optional.ofNullable(getStartKeys).orElse(getStartKeys);
    }

    @Override
    public byte[][] getEndKeys()
            throws IOException
    {
        byte[][] getEndKeys = null;
        return Optional.ofNullable(getEndKeys).orElse(getEndKeys);
    }

    @Override
    public Pair<byte[][], byte[][]> getStartEndKeys()
            throws IOException
    {
        Pair<byte[][], byte[][]> getStartEndKeys = null;
        return Optional.ofNullable(getStartEndKeys).orElse(getStartEndKeys);
    }

    @Override
    public TableName getName()
    {
        TableName tableName = null;
        return Optional.ofNullable(tableName).orElse(tableName);
    }

    @Override
    public void close()
            throws IOException
    {
        // do nothing
    }
}
