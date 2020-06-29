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
package io.hetu.core.plugin.hbase.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.JsonHBaseTableUtils;
import io.hetu.core.plugin.hbase.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * HdfsHBaseMetastore: store file on hdfs
 *
 * @since 2020-03-30
 */
public class HdfsHBaseMetastore
        implements HBaseMetastore
{
    private static final Logger LOG = Logger.get(HdfsHBaseMetastore.class);
    private static final String ID = "HDFS";

    private final Map<String, HBaseTable> hbaseTables = new ConcurrentHashMap<>();
    private String filePath;
    private HBaseConfig hbaseConfig;
    private FileSystem fs;
    private Configuration hdfsConfig;

    public HdfsHBaseMetastore(HBaseConfig config)
    {
        this.hbaseConfig = config;
        this.filePath = hbaseConfig.getMetastoreUrl();
        authenticate();
        this.fs = getFs();
    }

    @Override
    public void init()
    {
        JSONObject jsonObject = readJsonFromHdfs(filePath);
        JsonHBaseTableUtils.loadHBaseTablesFromJson(hbaseTables, jsonObject);
    }

    /**
     * get hdfs filesystem
     *
     * @return filesystem
     */
    private FileSystem getFs()
    {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(hdfsConfig);
        }
        catch (IOException e) {
            LOG.error("Get hadoop file System error, cause by %s", e.getMessage());
        }
        return fileSystem;
    }

    private void authenticate()
    {
        hdfsConfig = new Configuration();

        try {
            if (!Utils.isFileExist(hbaseConfig.getHdfsSitePath())) {
                throw new FileNotFoundException(hbaseConfig.getHdfsSitePath());
            }
            if (!Utils.isFileExist(hbaseConfig.getCoreSitePath())) {
                throw new FileNotFoundException(hbaseConfig.getCoreSitePath());
            }
            hdfsConfig.addResource(new Path(hbaseConfig.getHdfsSitePath()));
            hdfsConfig.addResource(new Path(hbaseConfig.getCoreSitePath()));

            if (Constants.HDFS_AUTHENTICATION_KERBEROS.equals(hbaseConfig.getKerberos())) {
                String keyTabPath = requireNonNull(hbaseConfig.getUserKeytabPath());
                String krb5ConfPath = requireNonNull(hbaseConfig.getKrb5ConfPath());

                if (!Utils.isFileExist(keyTabPath)) {
                    throw new FileNotFoundException(keyTabPath);
                }
                if (!Utils.isFileExist(krb5ConfPath)) {
                    throw new FileNotFoundException(krb5ConfPath);
                }

                String hdfsPrincipal = requireNonNull(hbaseConfig.getPrincipalUsername());
                System.setProperty("java.security.krb5.conf", krb5ConfPath);
                UserGroupInformation.setConfiguration(hdfsConfig);
                UserGroupInformation.loginUserFromKeytab(hdfsPrincipal, keyTabPath);
            }
        }
        catch (IOException exception) {
            if (exception instanceof FileNotFoundException) {
                LOG.error("File not exist, cause by %s", exception.getMessage());
            }
            else {
                LOG.error("Kerberos authentication failed, caused by %s", exception.getMessage());
            }
        }
    }

    @Override
    public String getId()
    {
        return this.ID;
    }

    @Override
    public void addHBaseTable(HBaseTable hBaseTable)
    {
        // hbaseTableJson must be synchronize, when save it to file ,it couldn't be modified
        synchronized (hbaseTables) {
            if (hbaseTables.containsKey(hBaseTable.getFullTableName())) {
                hbaseTables.remove(hBaseTable.getFullTableName());
            }

            hbaseTables.put(hBaseTable.getFullTableName(), hBaseTable);
            putJsonToHdfs(filePath, JsonHBaseTableUtils.hbaseTablesMapToJson(hbaseTables));
        }
    }

    @Override
    public void renameHBaseTable(HBaseTable newTable, String oldTable)
    {
        // hbaseTableJson must be synchronize, when save it to file ,it couldn't be modified
        synchronized (hbaseTables) {
            if (hbaseTables.containsKey(oldTable)) {
                hbaseTables.remove(oldTable);
            }

            hbaseTables.put(newTable.getFullTableName(), newTable);
            putJsonToHdfs(filePath, JsonHBaseTableUtils.hbaseTablesMapToJson(hbaseTables));
        }
    }

    @Override
    public void dropHBaseTable(HBaseTable hBaseTable)
    {
        synchronized (hbaseTables) {
            if (hbaseTables.containsKey(hBaseTable.getFullTableName())) {
                hbaseTables.remove(hBaseTable.getFullTableName());
                putJsonToHdfs(filePath, JsonHBaseTableUtils.hbaseTablesMapToJson(hbaseTables));
            }
        }
    }

    @Override
    public synchronized Map<String, HBaseTable> getAllHBaseTables()
    {
        return ImmutableMap.copyOf(hbaseTables);
    }

    @Override
    public HBaseTable getHBaseTable(String tableName)
    {
        return hbaseTables.get(tableName);
    }

    /**
     * readJsonFromHdfs
     *
     * @param file file
     * @return JSONObject
     */
    public JSONObject readJsonFromHdfs(String file)
    {
        InputStream input = null;
        JSONObject rs = null;
        StringBuilder catalogs = new StringBuilder();
        try {
            Path path = new Path(file);
            if (!fs.exists(path)) {
                return rs;
            }
            input = fs.open(path);
            while (input.available() > 0) {
                catalogs.append((char) input.read());
            }
        }
        catch (IOException e) {
            LOG.error("readJsonFromHdfs : some wrong...file [%s].", file);
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    LOG.error("readJsonFromHdfs : something wrong...file [%s].", file);
                }
            }
        }

        try {
            if (catalogs.toString().length() > 0) {
                rs = new JSONObject(catalogs.toString());
            }
        }
        catch (JSONException e) {
            LOG.error("readJsonFromHdfs : to json wrong...cause by [%s].", e.getMessage());
        }
        return rs;
    }

    /**
     * putJsonToFile
     *
     * @param file file
     * @param jsonObject jsonObject
     */
    public void putJsonToHdfs(String file, JSONObject jsonObject)
    {
        String jsonStr = "";
        OutputStream out = null;
        try {
            jsonStr = jsonObject.toString(Constants.NUMBER4);
        }
        catch (JSONException e) {
            LOG.error("putJsonToHdfs : json to string wrong...cause by [%s]", e.getMessage());
        }

        Path path = new Path(file);
        try {
            out = fs.create(path, true);
            BufferedWriter bufferOut = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
            bufferOut.write(jsonStr);
            bufferOut.flush();
            bufferOut.close();
        }
        catch (IOException e) {
            LOG.error("write file[%s] failed... ", e.getMessage());
        }
        finally {
            if (out != null) {
                try {
                    out.close();
                }
                catch (IOException e) {
                    LOG.error("write file[%s] failed... ", e.getMessage());
                }
            }
        }
    }
}
