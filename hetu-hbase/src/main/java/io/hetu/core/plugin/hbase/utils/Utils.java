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
package io.hetu.core.plugin.hbase.utils;

import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.hetu.core.plugin.hbase.utils.Constants.HBASE_DATA_TYPE_NAME_LIST;
import static java.util.Objects.requireNonNull;

/**
 * Utils
 *
 * @since 2020-03-18
 */
public class Utils
{
    private static final Logger LOG = Logger.get(Utils.class);

    private static final String KRB5_CONF_KEY = "java.security.krb5.conf";

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
     * generate hbase configuration
     *
     * @param hbaseConfig hbaseConfig
     * @return hbase configuration
     */
    public static Configuration generateHBaseConfig(HBaseConfig hbaseConfig) throws IOException
    {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseConfig.getZkQuorum());
        conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZkClientPort());
        conf.set("hbase.cluster.distributed", "true");
        conf.addResource(new Path(hbaseConfig.getCoreSitePath()));
        conf.addResource(new Path(hbaseConfig.getHdfsSitePath()));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        if (Constants.HDFS_AUTHENTICATION_KERBEROS.equals(hbaseConfig.getKerberos())) {
            String keyTab = requireNonNull(hbaseConfig.getUserKeytabPath(), "kerberos authentication was enabled but no keytab found ");
            String krb5 = requireNonNull(hbaseConfig.getKrb5ConfPath(), "kerberos authentication was enabled but no krb5.conf found ");
            String principle = requireNonNull(hbaseConfig.getPrincipalUsername(), "kerberos authentication was enabled but no principle found ");

            System.setProperty(KRB5_CONF_KEY, krb5);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principle, keyTab);
        }
        return conf;
    }

    /**
     * read the snapshot, get region infos.
     *
     * @param snapshotName snapshot name
     * @return region info list
     * @throws IOException IOException
     */
    public static List<RegionInfo> getRegionInfos(String snapshotName, HBaseConfig hbaseConfig)
    {
        try {
            Configuration conf = generateHBaseConfig(hbaseConfig);
            Path root = new Path(hbaseConfig.getZkZnodeParent());
            FileSystem fs = FileSystem.get(conf);
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
