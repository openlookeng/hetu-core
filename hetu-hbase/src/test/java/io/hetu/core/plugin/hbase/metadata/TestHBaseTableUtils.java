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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * TestJsonHBaseTableUtils
 *
 * @since 2020-03-20
 */
public class TestHBaseTableUtils
{
    private static final String TEST_METASTORE_FILE_PATH = "./hbasetablecatalogtmp.ini";
    private static LocalHBaseMetastore lHBMetastore = new LocalHBaseMetastore(TEST_METASTORE_FILE_PATH);
    private static String jsonStr =
            "{\n"
                    + "    \"hbase.test_table\": {\n"
                    + "        \"schema\": \"hbase\",\n"
                    + "        \"external\": false,\n"
                    + "        \"rowIdOrdinal\": 0,\n"
                    + "        \"serializerClassName\":"
                    + " \"io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer\",\n"
                    + "        \"indexed\": false,\n"
                    + "        \"rowId\": \"rowkey\",\n"
                    + "        \"table\": \"test_table\",\n"
                    + "        \"indexColumns\": \"\",\n"
                    + "        \"hbaseTableName\": \"hbase:test_table\",\n"
                    + "        \"columns\": [\n"
                    + "            {\n"
                    + "                \"name\": \"rowkey\",\n"
                    + "                \"family\": \"\",\n"
                    + "                \"qualifer\": \"\",\n"
                    + "                \"type\": \"io.prestosql.spi.type.VarcharType\",\n"
                    + "                \"ordinal\": 0,\n"
                    + "                \"comment\": \"HBase row ID\",\n"
                    + "                \"indexed\": false\n"
                    + "            },\n"
                    + "            {\n"
                    + "                \"name\": \"name\",\n"
                    + "                \"family\": \"name\",\n"
                    + "                \"qualifer\": \"nick_name\",\n"
                    + "                \"type\": \"io.prestosql.spi.type.VarcharType\",\n"
                    + "                \"ordinal\": 1,\n"
                    + "                \"comment\": \"HBase column name:nick_name. Indexed: false\",\n"
                    + "                \"indexed\": false\n"
                    + "            },\n"
                    + "            {\n"
                    + "                \"name\": \"age\",\n"
                    + "                \"family\": \"age\",\n"
                    + "                \"qualifer\": \"lit_age\",\n"
                    + "                \"type\": \"io.prestosql.spi.type.BigintType\",\n"
                    + "                \"ordinal\": 2,\n"
                    + "                \"comment\": \"HBase column age:lit_age. Indexed: false\",\n"
                    + "                \"indexed\": false\n"
                    + "            },\n"
                    + "            {\n"
                    + "                \"name\": \"gender\",\n"
                    + "                \"family\": \"gender\",\n"
                    + "                \"qualifer\": \"gender\",\n"
                    + "                \"type\": \"io.prestosql.spi.type.DateType\",\n"
                    + "                \"ordinal\": 3,\n"
                    + "                \"comment\": \"HBase column gender:gender. Indexed: false\",\n"
                    + "                \"indexed\": false\n"
                    + "            },\n"
                    + "            {\n"
                    + "                \"name\": \"t\",\n"
                    + "                \"family\": \"t\",\n"
                    + "                \"qualifer\": \"t\",\n"
                    + "                \"type\": \"io.prestosql.spi.type.BigintType\",\n"
                    + "                \"ordinal\": 4,\n"
                    + "                \"comment\": \"HBase column t:t. Indexed: false\",\n"
                    + "                \"indexed\": false\n"
                    + "            }\n"
                    + "        ]\n"
                    + "    }}";

    private TestHBaseTableUtils() {}

    /**
     * preFile
     *
     * @throws JSONException Exception
     */
    public static void preFile(String file)
    {
        try {
            JSONObject json = new JSONObject(jsonStr);
            lHBMetastore.putJsonToFile(file, json);
        }
        catch (JSONException e) {
            assertEquals(e.toString().substring(0, 36), "putJsonToFile : json to string wrong");
        }
    }

    /**
     * delFile
     */
    public static void delFile(String file)
    {
        File pfile = new File(file);
        pfile.delete();
    }

    /**
     * createFile
     */
    public static void createFile(String filename)
            throws Exception
    {
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            }
            catch (IOException e) {
                throw new IOException("createFile " + filename + " : failed");
            }
        }
    }
}
