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

package io.hetu.core.plugin.carbondata.integrationtest;

import io.hetu.core.plugin.carbondata.server.HetuTestServer;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestsWithHiveConnector
{
    private String rootPath = new File(this.getClass().getResource("/").getPath() + "../..")
            .getCanonicalPath();

    private HetuTestServer hetuServer = new HetuTestServer();
    private String storePath = rootPath + "/target/store_new";

    public TestsWithHiveConnector()
            throws Exception
    {
    }

    @BeforeClass
    public void setup() throws Exception
    {
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "HetuTest");

        Map<String, String> map = new HashMap<String, String>();
        map.put("hive.metastore", "file");
        map.put("hive.allow-drop-table", "true");
        map.put("hive.non-managed-table-writes-enabled", "true");
        map.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        map.put("carbondata.store-location", "file://" + storePath + "/carbon.store");

        if (!FileFactory.isFileExist( storePath + "/carbon.store")) {
            FileFactory.mkdirs( storePath + "/carbon.store");
        }

        hetuServer.startServer("default", map);
        hetuServer.execute("drop schema if exists default");
        hetuServer.execute("create schema default");
        hetuServer.addHiveCatalogToQueryRunner(createHiveProperties());
    }

    @AfterClass
    public void tearDown() throws SQLException, IOException, InterruptedException
    {
        hetuServer.stopServer();
        CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath));
    }

    @Test
    public void block_Hive_Table_from_Carbondata()
            throws SQLException
    {
        hetuServer.execute("CREATE TABLE hive.default.demotable (c1 int)");

        hetuServer.execute("use carbondata.default");

        runQueryAndAssertErrorMessage("SELECT * FROM demotable",
                "Carbondata connector can only read carbondata tables");

        runQueryAndAssertErrorMessage("INSERT INTO demotable VALUES(1)",
                "Tables with OrcInputFormat are not supported by Carbondata connector");

        runQueryAndAssertErrorMessage("UPDATE demotable SET c1=1",
                "Tables with OrcInputFormat are not supported by Carbondata connector");

        runQueryAndAssertErrorMessage("DELETE FROM demotable",
                "Tables with OrcInputFormat are not supported by Carbondata connector");

        hetuServer.execute("DROP TABLE hive.default.demotable");
    }

    @Test
   public void deleteTransactionTableDirc()
            throws SQLException
    {
        hetuServer.execute("drop table if exists hive.default.parttable");
        hetuServer.execute("set session DELETE_TRANSACTIONAL_TABLE_DIRECT = true");
        hetuServer.execute("create table hive.default.parttable (orderkey int, year int) WITH (transactional = true , format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        hetuServer.execute("insert into hive.default.parttable values (1,2011)");
        hetuServer.execute("insert into hive.default.parttable values (2,2012)");
        hetuServer.execute("insert into hive.default.parttable values (3,2013)");
        hetuServer.execute("delete from hive.default.parttable where year =2013");
        try {
        assertEquals(FileFactory.isFileExist(storePath +
                "hive.store/default/parttable/year=2013", false), false);
        } catch (IOException exception) {
            exception.printStackTrace();
        }
        hetuServer.execute("DROP TABLE hive.default.parttable");
    }

    @Test
    public void deleteTransactionTableDirUsing2tables()
            throws SQLException
    {
        hetuServer.execute("drop table if exists hive.default.parttable");
        hetuServer.execute("drop table if exists hive.default.parttable1");
        hetuServer.execute("set session DELETE_TRANSACTIONAL_TABLE_DIRECT = true");
        hetuServer.execute("create table hive.default.parttable (orderkey int, year int) WITH (transactional = true , format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        hetuServer.execute("insert into hive.default.parttable values (1,2011)");
        hetuServer.execute("insert into hive.default.parttable values (2,2012)");
        hetuServer.execute("insert into hive.default.parttable values (3,2013)");

        hetuServer.execute("create table hive.default.parttable1 (orderkey int, year int) WITH (transactional = true , format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        hetuServer.execute("insert into hive.default.parttable1 values (1,2011)");
        hetuServer.execute("insert into hive.default.parttable1 values (2,2012)");
        hetuServer.execute("insert into hive.default.parttable1 values (3,2013)");

        hetuServer.execute("delete from hive.default.parttable where year >= (select max(year) from hive.default.parttable1)  ");
        try {
            assertEquals(FileFactory.isFileExist(storePath +
                    "hive.store/default/parttable/year=2013", false), false);
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        hetuServer.execute("insert into hive.default.parttable values (4,2014)");
        hetuServer.execute("delete from hive.default.parttable where year >= (select year from hive.default.parttable1 where orderkey=4 )  ");
        try {
            assertEquals(FileFactory.isFileExist(storePath +
                    "hive.store/default/parttable/year=2014", false), false);
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        hetuServer.execute("DROP TABLE hive.default.parttable");
        hetuServer.execute("DROP TABLE hive.default.parttable1");
    }

    @Test
    public void deleteTransactionTableDirDisable()
            throws SQLException
    {
        hetuServer.execute("drop table if exists hive.default.parttable");
        hetuServer.execute("set session DELETE_TRANSACTIONAL_TABLE_DIRECT = false");
        hetuServer.execute("create table hive.default.parttable (orderkey int, year int) WITH (transactional = true , format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        hetuServer.execute("insert into hive.default.parttable values (1,2011)");
        hetuServer.execute("insert into hive.default.parttable values (2,2012)");
        hetuServer.execute("insert into hive.default.parttable values (3,2013)");

        hetuServer.execute("create table hive.default.parttable1 (orderkey int, year int) WITH (transactional = true , format = 'ORC', partitioned_by = ARRAY[ 'year' ] )");
        hetuServer.execute("insert into hive.default.parttable1 values (1,2011)");
        hetuServer.execute("insert into hive.default.parttable1 values (2,2012)");
        hetuServer.execute("insert into hive.default.parttable1 values (3,2013)");

        hetuServer.execute("delete from hive.default.parttable where year >= (select max(year) from hive.default.parttable1)  ");
        try {
            assertEquals(FileFactory.isFileExist(storePath +
                    "/hive.store/default/parttable/year=2013", false), true);
        } catch (IOException exception) {
            exception.printStackTrace();
        }
        hetuServer.execute("DROP TABLE hive.default.parttable");
    }

    @Test(dependsOnMethods = {"block_Hive_Table_from_Carbondata"})
    public void block_Carbondata_Table_from_Hive()
            throws SQLException
    {
        hetuServer.execute("CREATE TABLE carbondata.default.demotable (c1 int)");

        hetuServer.execute("use hive.default");

        runQueryAndAssertErrorMessage("SELECT * FROM demotable",
                "Hive connector can't read carbondata tables");

        runQueryAndAssertErrorMessage("INSERT INTO demotable VALUES(1)",
                "Tables with MapredCarbonInputFormat are not supported by Hive connector");

        runQueryAndAssertErrorMessage("UPDATE demotable SET c1=1",
                "Tables with MapredCarbonInputFormat are not supported by Hive connector");

        hetuServer.execute("DROP TABLE hive.default.demotable");
    }

    private Map<String, String> createHiveProperties()
    {
        Map<String, String> hiveProperties = new HashMap<String, String>();
        hiveProperties.put("hive.metastore", "file");
        hiveProperties.put("hive.allow-drop-table", "true");
        hiveProperties.put("hive.non-managed-table-writes-enabled", "true");
        hiveProperties.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        return hiveProperties;
    }

    private void runQueryAndAssertErrorMessage(String query, String errorMessage)
    {
        assertThrows(SQLException.class, () -> {
            try {
                hetuServer.execute(query);
            }
            catch (Exception e) {
                assertTrue(e.getMessage().contains(errorMessage));
                throw e;
            }
        });
    }
}
