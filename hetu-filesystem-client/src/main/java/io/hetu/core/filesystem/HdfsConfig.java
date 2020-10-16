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
package io.hetu.core.filesystem;

import io.airlift.log.Logger;
import io.hetu.core.common.util.SecurePathWhiteList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A class which holds the configuration for a HetuHdfsFileSystemClient
 *
 * @since 2020-03-30
 */
public class HdfsConfig
{
    private static final Logger LOG = Logger.get(HetuHdfsFileSystemClient.class);
    private static final String HDFS_CONFIG_RESOURCES = "hdfs.config.resources";
    private static final String HDFS_AUTHENTICATION_TYPE = "hdfs.authentication.type";
    private static final String HDFS_AUTHENTICATION_TYPE_KERBEROS = "KERBEROS";
    private static final String HDFS_AUTHENTICATION_TYPE_NONE = "NONE";
    private static final String HDFS_KRB5_CONFIG_PATH = "hdfs.krb5.conf.path";
    private static final String HDFS_KRB5_KEYTAB_PATH = "hdfs.krb5.keytab.path";
    private static final String HDFS_KRB5_PRINCIPAL = "hdfs.krb5.principal";
    private static final String KRB5_CONF_KEY = "java.security.krb5.conf";
    private static final String FS_AUTOMATIC_CLOSE = "hdfs.fs.automatic.close";
    private static final String FS_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private Configuration hadoopConfig;

    /**
     * Create an HdfsConfig object from raw properties.
     *
     * @param properties Properties for Hdfs configuration, including config resources, authentication files, etc.
     * @throws IOException Exception when reading hdfs config files.
     */
    public HdfsConfig(Properties properties)
            throws IOException
    {
        hadoopConfig = generateHadoopConfig(properties);
    }

    /**
     * Wrap a Hadoop Configuration object directly. No need to generate inside this class.
     *
     * @param hadoopConfig Hadoop Configuration object to be wrapped.
     */
    public HdfsConfig(Configuration hadoopConfig)
    {
        this.hadoopConfig = hadoopConfig;
    }

    /**
     * Getter for hadoop configuration.
     *
     * @return Hadoop configuration object constructed from properties extracted from config.properties file
     * @throws IOException thrown when generating the Hadoop configuration object
     */
    public Configuration getHadoopConfig()
    {
        return hadoopConfig;
    }

    private Configuration generateHadoopConfig(Properties properties)
            throws IOException
    {
        String configResources = properties.getProperty(HDFS_CONFIG_RESOURCES);
        requireNonNull(configResources, "no hadoop config resources found ");

        String[] parts = configResources.split(",");

        Configuration config = new Configuration();

        // disable automatically closing the FileSystem in shutdown
        config.setBoolean(FS_AUTOMATIC_CLOSE, false);

        String disableCache = properties.getProperty(FS_DISABLE_CACHE, "false");
        if (disableCache.equalsIgnoreCase("true")) {
            config.setBoolean(FS_DISABLE_CACHE, true);
        }

        for (String resourcePath : parts) {
            String trimmedResourcePath = resourcePath.trim();
            if (!"".equals(resourcePath)) {
                checkConfigFile(trimmedResourcePath);
                config.addResource(new org.apache.hadoop.fs.Path(trimmedResourcePath));
            }
        }

        String authType = properties.getProperty(HDFS_AUTHENTICATION_TYPE);
        if (HDFS_AUTHENTICATION_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
            String keytabFile = properties.getProperty(HDFS_KRB5_KEYTAB_PATH);
            requireNonNull(keytabFile, "kerberos authentication was enabled but no keytab found ");

            checkConfigFile(keytabFile);

            String krb5ConfigFile = properties.getProperty(HDFS_KRB5_CONFIG_PATH);
            requireNonNull(krb5ConfigFile, "kerberos authentication was enabled but no krb5.conf found ");

            checkConfigFile(krb5ConfigFile);

            String principle = properties.getProperty(HDFS_KRB5_PRINCIPAL);
            requireNonNull(principle, "kerberos authentication was enabled but principle found ");

            getKerberosToken(config, krb5ConfigFile, keytabFile, principle);
        }
        else if (HDFS_AUTHENTICATION_TYPE_NONE.equalsIgnoreCase(authType)) {
            LOG.debug("Authentication type NONE, no Kerberos token generated");
        }
        else {
            throw new IOException("Unsupported authentication type: " + authType);
        }

        return config;
    }

    /**
     * get a Kerberos Token
     *
     * @param conf hadoop config
     * @param krb5ConfigFilePath krb5 config file
     * @param keytabFilePath keytab for Kerberos user
     * @param principle principle for Kerberos user
     * @throws IOException when fails
     */
    private synchronized void getKerberosToken(Configuration conf, String krb5ConfigFilePath,
            String keytabFilePath, String principle)
            throws IOException
    {
        System.setProperty(KRB5_CONF_KEY, krb5ConfigFilePath);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principle, keytabFilePath);
    }

    private void checkConfigFile(String path)
    {
        try {
            checkArgument(!path.contains("../"),
                    path + "Path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(path),
                    path + "Path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }

        checkArgument(new File(path).exists(), String.format(Locale.ROOT, "%s is not found", path));
    }
}
