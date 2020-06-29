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

package io.hetu.core.heuristicindex.hdfs;

import io.hetu.core.spi.heuristicindex.IndexStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Persistence class for storing data into HDFS
 */
public class HdfsIndexStore
        implements IndexStore
{
    private static final Logger LOG = LoggerFactory.getLogger(HdfsIndexStore.class);

    private static final String HDFS_CONFIG_RESOURCES = "hdfs.config.resources";
    private static final String HDFS_AUTHENTICATION_TYPE = "hdfs.authentication.type";
    private static final String HDFS_AUTHENTICATION_TYPE_KERBEROS = "KERBEROS";
    private static final String HDFS_AUTHENTICATION_TYPE_NONE = "NONE";
    private static final String HDFS_KRB5_CONFIG_PATH = "hdfs.krb5.conf.path";
    private static final String HDFS_KRB5_KEYTAB_PATH = "hdfs.krb5.keytab.path";
    private static final String HDFS_KRB5_PRINCIPAL = "hdfs.krb5.principal";
    private static final String KRB5_CONF_KEY = "java.security.krb5.conf";
    private static final String FS_AUTOMATIC_CLOSE = "fs.automatic.close";
    private static final String ID = "HDFS";

    private FileSystem fs;

    private Configuration config;
    private Properties properties;

    /**
     * Default constructor
     */
    public HdfsIndexStore()
    {
    }

    /**
     * Instantiate an HdfsIndexStore instance with the passed in parameters
     *
     * @param properties    IndexStore properties extracted form config.properties file
     * @param configuration Hadoop configuration object
     * @param fs            Hadoop filesystem object
     */
    public HdfsIndexStore(Properties properties, Configuration configuration, FileSystem fs)
    {
        setProperties(properties);
        setConfiguration(configuration);
        setFs(fs);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public boolean delete(String path) throws IOException
    {
        Path p = new Path(path);
        return getFs().delete(p, true);
    }

    @Override
    public void write(String content, String path, boolean isOverwritable) throws IOException
    {
        Path p = new Path(path);
        try (OutputStream os = getFs().create(p, isOverwritable)) {
            write(content, os);
        }
    }

    @Override
    public OutputStream getOutputStream(String path, boolean isOverwritable) throws IOException
    {
        Path p = new Path(path);
        if (!isOverwritable && getFs().exists(p)) {
            return getFs().append(p);
        }
        else {
            return getFs().create(p, isOverwritable);
        }
    }

    @Override
    public InputStream read(String path) throws IOException
    {
        Path p = new Path(path);
        return getFs().open(p);
    }

    @Override
    public boolean exists(String path) throws IOException
    {
        Path p = new Path(path);
        return getFs().exists(p);
    }

    @Override
    public boolean renameTo(String curPath, String newPath) throws IOException
    {
        Path srcPath = new Path(curPath);
        Path dstPath = new Path(newPath);
        return getFs().rename(srcPath, dstPath);
    }

    @Override
    public long getLastModifiedTime(String path) throws IOException
    {
        FileStatus fstatus = getFs().getFileStatus(new Path(path));
        return fstatus.getModificationTime();
    }

    @Override
    public Collection<String> listChildren(String path, boolean isRecursive) throws IOException
    {
        Set<String> files = new HashSet<>(1);
        RemoteIterator<LocatedFileStatus> iterator = getFs().listFiles(new Path(path), isRecursive);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            // turn to URI and get path, this removes the protocol prefix, host, and port, i.e. hdfs://hadoop:9000
            // note: just calling uriObj.getPath() removes anything after #
            URI uriObj = URI.create(fileStatus.getPath().toString());
            String uriString = uriObj.toString();
            if (uriObj.getAuthority() != null) {
                uriString = uriString.substring(uriString.indexOf(uriObj.getAuthority()) + uriObj.getAuthority().length());
            }
            files.add(uriString);
        }
        return files;
    }

    /**
     * Getter for configuration. (lazy instantiation)
     *
     * @return Hadoop configuration object constructed from properties extracted from config.properties file
     * @throws IOException thrown when generating the Hadoop configuration object
     */
    public Configuration getConfiguration() throws IOException
    {
        if (config == null) {
            config = generateHadoopConfig(getProperties());
        }

        return config;
    }

    public void setConfiguration(Configuration newConfig)
    {
        this.config = newConfig;
    }

    public Properties getProperties()
    {
        return properties;
    }

    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    /**
     * Getter for filesystem object (lazy instantiation)
     *
     * @return filesystem object configured through the Hadoop configuration object
     * @throws IOException thrown when getting the Hadoop configuration object
     */
    public FileSystem getFs() throws IOException
    {
        if (fs == null) {
            this.fs = FileSystem.get(getConfiguration());
        }
        return fs;
    }

    public void setFs(FileSystem fs)
    {
        this.fs = fs;
    }

    /**
     * Generates a Hadoop config based on the properties.
     *
     * @param properties properties should include the hdfs resources and authentication info
     * @return Hadoop Configuration object using the given <code>properties</code>
     * @throws IOException thrown during generating Kerberos token
     */
    private static Configuration generateHadoopConfig(Properties properties) throws IOException
    {
        String configResources = properties.getProperty(HDFS_CONFIG_RESOURCES);
        requireNonNull(configResources, "no hadoop config resources found ");

        String[] parts = configResources.split(",");

        Configuration config = new Configuration();

        // disable automatically closing the FileSystem in shutdown
        config.setBoolean(FS_AUTOMATIC_CLOSE, false);

        for (String resourcePath : parts) {
            String trimmedResourcePath = resourcePath.trim();
            IndexServiceUtils.isFileExisting(trimmedResourcePath);
            config.addResource(new Path(trimmedResourcePath));
        }

        String authType = properties.getProperty(HDFS_AUTHENTICATION_TYPE);
        if (HDFS_AUTHENTICATION_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
            String keytabFile = properties.getProperty(HDFS_KRB5_KEYTAB_PATH);
            requireNonNull(keytabFile, "kerberos authentication was enabled but no keytab found ");

            IndexServiceUtils.isFileExisting(keytabFile);

            String krb5ConfigFile = properties.getProperty(HDFS_KRB5_CONFIG_PATH);
            requireNonNull(krb5ConfigFile, "kerberos authentication was enabled but no krb5.conf found ");

            IndexServiceUtils.isFileExisting(krb5ConfigFile);

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
     * @param conf               hadoop config
     * @param krb5ConfigFilePath krb5 config file
     * @param keytabFilePath     keytab for Kerbros user
     * @param principle          principle for Kerbros user
     * @throws IOException when fails
     */
    private static synchronized void getKerberosToken(Configuration conf, String krb5ConfigFilePath,
                                                      String keytabFilePath, String principle) throws IOException
    {
        System.setProperty(KRB5_CONF_KEY, krb5ConfigFilePath);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principle, keytabFilePath);
    }

    /**
     * Close the hadoop FileSystem object
     *
     * @throws IOException thrown by closing the FileSystem object
     */
    @Override
    public void close() throws IOException
    {
        getFs().close();
    }
}
