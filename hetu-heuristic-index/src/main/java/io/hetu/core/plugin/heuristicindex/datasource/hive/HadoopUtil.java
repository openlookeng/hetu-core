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
package io.hetu.core.plugin.heuristicindex.datasource.hive;

import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static io.hetu.core.plugin.heuristicindex.datasource.hive.ConstantsHelper.KRB5_CONF_KEY;
import static java.util.Objects.requireNonNull;

/**
 * Utility class for Hadoop related helper methods such as generating the kerberized Hadoop configuration
 * and getting the FileStatus from HDFS, etc.
 *
 * @since 2019-10-10
 */
public class HadoopUtil
{
    private HadoopUtil()
    {
    }

    /**
     * Gets the table metadata through Thrift service calls to Hive
     *
     * @param database   database name
     * @param table      table name
     * @param properties Hive properties object (usually in hive.properties file)
     * @return the corresponding {@link TableMetadata} object for the given database.table
     */
    public static TableMetadata getTableMetadata(String database, String table, Properties properties)
    {
        ThriftHiveMetaStoreServiceFactory factory = ThriftHiveMetaStoreServiceFactory.getInstance();
        ThriftHiveMetaStoreService metaStoreService = factory.getMetaStoreService(properties);
        TableMetadata tableMetadata = metaStoreService.getTableMetadata(database, table);

        return tableMetadata;
    }

    /**
     * Generates the Hadoop configuration object from the given properties. The generated object can be used to
     * construct Hadoop FileSystem object with or without Kerberos authentication.
     *
     * @param properties Properties object containing the necessary information, usually found in hive.properties
     * @return Hadoop configuration object constructed using the properties
     * @throws IOException thrown when getting the Kerberos token
     */
    public static Configuration generateHadoopConfig(Properties properties) throws IOException
    {
        String configResources = properties.getProperty(ConstantsHelper.HIVE_CONFIG_RESOURCES);
        requireNonNull(configResources, "no hadoop config resources found ");

        String[] parts = configResources.split(",");

        Configuration config = new Configuration();

        for (String path : parts) {
            String resourcePath = path.trim();
            IndexServiceUtils.isFileExisting(resourcePath);
            config.addResource(new Path(resourcePath));
        }

        if (ConstantsHelper.HDFS_AUTHENTICATION_KERBEROS.equals(
                properties.getProperty(ConstantsHelper.HDFS_AUTHENTICATION_TYPE))) {
            String keytabFile = properties.getProperty(ConstantsHelper.HDFS_KEYTAB_FILEPATH);
            requireNonNull(keytabFile, "kerberos authentication was enabled but no keytab found ");

            IndexServiceUtils.isFileExisting(keytabFile);

            String krb5ConfigFile = properties.getProperty(ConstantsHelper.HIVE_METASTORE_KRB5_CONF, System.getProperty(KRB5_CONF_KEY));
            requireNonNull(krb5ConfigFile, "kerberos authentication was enabled but no krb5.conf found. " +
                    "Either " + ConstantsHelper.HIVE_METASTORE_KRB5_CONF + " must be set in the catalog file or " +
                    KRB5_CONF_KEY + " system property must be set.");

            IndexServiceUtils.isFileExisting(krb5ConfigFile);

            String principle = properties.getProperty(ConstantsHelper.HDFS_AUTH_PRINCIPLE);
            requireNonNull(principle, "kerberos authentication was enabled but no principle found ");

            HiveIndexServiceUtil.getKerberosToken(config, krb5ConfigFile, keytabFile, principle);
        }

        return config;
    }

    /**
     * Recursively get all files at the filePath that are in the provided partitions,
     * match all files if partitions is null
     *
     * @param fs                   Hadoop {@link FileSystem} object that is used for connecting to HDFS
     * @param filePath             Path to the directory/file to be listed
     * @param partitions           Optional, if passed in only the files under the directories with the partition names
     *                             will be listed
     * @param isTransactionalTable
     * @return A list of {@link FileStatus} objects under the given filePath and, if passed in, the given
     * partition folders
     * @throws IOException Thrown when reading the files/directories from HDFS
     */
    public static List<FileStatus> getFiles(FileSystem fs, Path filePath, String[] partitions,
                                            boolean isTransactionalTable) throws IOException
    {
        List<FileStatus> files = new LinkedList<>();

        FileStatus[] fileStatuses = fs.listStatus(filePath);
        for (FileStatus fileStatus : fileStatuses) {
            // Ignore hidden files and directories. Hive ignores files starting with _ and . as well.
            String fileName = fileStatus.getPath().getName();
            if (isTransactionalTable && canSkipFile(fileName)) {
                continue;
            }
            if (fileStatus.isDirectory()) {
                files.addAll(getFiles(fs, fileStatus.getPath(), partitions, isTransactionalTable));
            }
            else if (matchPartitions(fileStatus, partitions)) {
                // If it's a file, we also need to check whether it is under one of the given partitions
                files.add(fileStatus);
            }
        }

        return files;
    }

    private static boolean canSkipFile(String fileName)
    {
        return fileName.startsWith("_") || fileName.startsWith(".") || fileName.startsWith("delete_delta_");
    }

    /**
     * <pre>
     * Pre: Resource path must point to a file
     * Check whether the resource belongs to at least one of the partition of the partition list
     * <i>Note: the file does not have to belong to all the partitions passed in</i>
     * </pre>
     *
     * @param fileStatus Path to a file
     * @param partitions Partition list
     * @return True if the file belongs to at least one of the partition in the list or the list is empty
     */
    protected static boolean matchPartitions(FileStatus fileStatus, String[] partitions)
    {
        if (partitions == null || partitions.length == 0) {
            return true;
        }
        String uri = fileStatus.getPath().toString();

        for (String partition : partitions) {
            if (uri.contains(partition + "/")) {
                return true;
            }
        }
        return false;
    }
}
