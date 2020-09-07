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
import io.hetu.core.plugin.hbase.utils.Constants;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LocalHBaseMetastore: store file on local filesystem
 * This Metastore is just for test
 *
 * @since 2020-03-30
 */
public class LocalHBaseMetastore
        implements HBaseMetastore
{
    private static final Logger LOG = Logger.get(LocalHBaseMetastore.class);
    private static final String ID = "LOCAL";
    private final Map<String, HBaseTable> hbaseTables = new ConcurrentHashMap<>();
    private final String filePath;

    public LocalHBaseMetastore(String filePath)
    {
        this.filePath = filePath;
    }

    @Override
    public void init()
    {
        JSONObject jsonObject = readJsonFromFile(filePath);
        JsonHBaseTableUtils.loadHBaseTablesFromJson(hbaseTables, jsonObject);
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
            putJsonToFile(filePath, JsonHBaseTableUtils.hbaseTablesMapToJson(hbaseTables));
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
            putJsonToFile(filePath, JsonHBaseTableUtils.hbaseTablesMapToJson(hbaseTables));
        }
    }

    @Override
    public void dropHBaseTable(HBaseTable hBaseTable)
    {
        synchronized (hbaseTables) {
            if (hbaseTables.containsKey(hBaseTable.getFullTableName())) {
                hbaseTables.remove(hBaseTable.getFullTableName());
                putJsonToFile(filePath, JsonHBaseTableUtils.hbaseTablesMapToJson(hbaseTables));
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
     * readJsonFromFile
     *
     * @param file file
     * @return JSONObject
     */
    public JSONObject readJsonFromFile(String file)
    {
        Reader in = null;
        JSONObject rs = null;
        StringBuilder catalogs = new StringBuilder();
        try {
            File metaFile = new File(file);
            if (!metaFile.exists()) {
                return rs;
            }
            char[] buffer = new char[Constants.NUMBER1024];
            in = new FileReader(file);
            int len = in.read(buffer);
            while (len != Constants.NUMBER_NEGATIVE_1) {
                if (len == Constants.NUMBER1024) {
                    catalogs.append(buffer);
                }
                else {
                    catalogs.append(buffer, 0, len);
                }
                len = in.read(buffer);
            }
        }
        catch (IOException e) {
            LOG.error("readJsonFromFile : some wrong...file [%s]. Cause by %s", file, e.getMessage());
        }
        finally {
            if (in != null) {
                try {
                    in.close();
                }
                catch (IOException e) {
                    LOG.error("readJsonFromFile : something wrong...file [%s]. Cause by %s", file, e.getMessage());
                }
            }
        }

        try {
            if (catalogs.toString().length() > 0) {
                rs = new JSONObject(catalogs.toString());
            }
        }
        catch (JSONException e) {
            LOG.error("readJsonFromFile : to json wrong...file[%s]. Cause by %s", file, e.getMessage());
        }
        return rs;
    }

    /**
     * putJsonToFile
     *
     * @param file file
     * @param jsonObject jsonObject
     */
    public void putJsonToFile(String file, JSONObject jsonObject)
    {
        PrintWriter out = null;
        String jsonStr = "";

        try {
            jsonStr = jsonObject.toString(Constants.NUMBER4);
        }
        catch (JSONException e) {
            LOG.error("putJsonToFile : json to string wrong...cause by", e.getMessage());
        }

        // check file exist and file permission
        File metaFile = new File(file);
        if (!metaFile.exists()) {
            try {
                if (!metaFile.createNewFile()) {
                    throw new IOException("when create file, it return false");
                }
            }
            catch (IOException e) {
                LOG.error("create file[%s] failed... check permission is available", e);
            }
        }

        try {
            // overwrite
            out = new PrintWriter(new FileWriter(file, false));
            // write file by json format
            out.write(jsonStr);
            out.flush();
        }
        catch (IOException e) {
            LOG.error("putJsonToFile : some wrong...cause by", e.getMessage());
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
