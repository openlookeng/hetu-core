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
package io.hetu.core.plugin.hbase.utils;

import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.hetu.core.plugin.hbase.utils.Constants.HBASE_DATA_TYPE_NAME_LIST;

/**
 * Utils
 *
 * @since 2020-03-18
 */
public class Utils
{
    private static final Logger LOG = Logger.get(Utils.class);

    private Utils() {}

    /**
     * Whether sql constraint contains conditions like "rowKey='xxx'" or "rowKey in ('xxx','xxx')"
     *
     * @param tupleDomain TupleDomain
     * @param rowIdOrdinal int
     * @return true if this sql is batch get.
     */
    public static boolean isBatchGet(TupleDomain<ColumnHandle> tupleDomain, int rowIdOrdinal)
    {
        if (tupleDomain != null && tupleDomain.getDomains().isPresent()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            HBaseColumnHandle columnHandle =
                    domains.keySet().stream()
                            .map(key -> (HBaseColumnHandle) key)
                            .filter((key -> key.getOrdinal() == rowIdOrdinal))
                            .findAny()
                            .orElse(null);

            if (columnHandle != null) {
                for (Range range : domains.get(columnHandle).getValues().getRanges().getOrderedRanges()) {
                    if (range.isSingleValue()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * createTypeByName
     *
     * @param type String
     * @return Type
     */
    public static Type createTypeByName(String type)
    {
        Type result = null;
        if (!HBASE_DATA_TYPE_NAME_LIST.contains(type)) {
            return Optional.ofNullable(result).orElse(result);
        }
        try {
            Class clazz = Class.forName(type);
            Field[] fields = clazz.getFields();

            for (Field field : fields) {
                if (type.equals(field.getType().getName())) {
                    Object object = field.get(clazz);
                    if (object instanceof Type) {
                        return (Type) object;
                    }
                }
            }
        }
        catch (ClassNotFoundException | IllegalAccessException e) {
            LOG.error("createTypeByName failed... cause by : %s", e);
        }

        return Optional.ofNullable(result).orElse(result);
    }

    /**
     * check file exist
     *
     * @param path file path
     * @return true/false
     */
    public static boolean isFileExist(String path)
    {
        File file = new File(path);
        return file.exists();
    }

    /**
     * read the snapshot, get region infos.
     *
     * @param snapshotName snapshot name
     * @return region info list
     * @throws IOException IOException
     */
    public static List<RegionInfo> getRegionInfos(String snapshotName, HBaseConnection hbaseConnection)
    {
        try {
            Configuration conf = hbaseConnection.getConfiguration();
            Path root = new Path(hbaseConnection.getHbaseConfig().getZkZnodeParent());
            FileSystem fs = hbaseConnection.getFileSystem();
            Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, root);
            SnapshotProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
            SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

            return getRegionInfoFromManifest(manifest);
        }
        catch (IOException ex) {
            LOG.error("get region info error: " + ex.getMessage(), ex);
            throw new UncheckedIOException("get region info error: " + snapshotName, ex);
        }
    }

    /**
     * get region infos from manifest.
     *
     * @param manifest manifest
     * @return region info list
     */
    public static List<RegionInfo> getRegionInfoFromManifest(SnapshotManifest manifest)
    {
        List<RegionInfo> regionInfos = new ArrayList<>();
        List<SnapshotProtos.SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
        if (regionManifests == null) {
            throw new IllegalArgumentException("Snapshot seems empty");
        }
        for (SnapshotProtos.SnapshotRegionManifest regionManifest : regionManifests) {
            RegionInfo hri = ProtobufUtil.toRegionInfo(regionManifest.getRegionInfo());
            if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
                continue;
            }
            regionInfos.add(hri);
        }
        return regionInfos;
    }
}
