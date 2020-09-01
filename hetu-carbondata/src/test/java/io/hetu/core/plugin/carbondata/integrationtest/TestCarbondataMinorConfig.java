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
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestCarbondataMinorConfig
{
    private final Logger logger = LogServiceFactory.getLogService(TestCarbondataMinorConfig.class.getCanonicalName());

    private String rootPath = new File(this.getClass().getResource("/").getPath() + "../..")
            .getCanonicalPath();

    private String storePath = rootPath + "/target/store_test";
    private HetuTestServer hetuServer = new HetuTestServer();

    public TestCarbondataMinorConfig()
            throws Exception
    {
    }

    @BeforeClass
    public void setup() throws Exception
    {
        logger.info("Setup begin: " + this.getClass().getSimpleName());

        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "HetuTest2");

        Map<String, String> map = new HashMap<String, String>();
        map.put("hive.metastore", "file");
        map.put("hive.allow-drop-table", "true");
        map.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        map.put("carbondata.store-location", "file://" + storePath + "/carbon.store");
        map.put("carbondata.minor-vacuum-seg-count", "-1");

        if (!FileFactory.isFileExist( storePath + "/carbon.store")) {
            FileFactory.mkdirs( storePath + "/carbon.store");
        }

        hetuServer.startServer("mytestdb", map);
        hetuServer.execute("drop schema if exists mytestdb");
        hetuServer.execute("drop schema if exists default");
        hetuServer.execute("create schema mytestdb");
        hetuServer.execute("create schema default");

        logger.info("CarbonStore created at location : " + storePath);
    }

    @AfterClass
    public void tearDown() throws SQLException, IOException, InterruptedException
    {
        logger.info("TearDown begin: " + this.getClass().getSimpleName());
        hetuServer.stopServer();
        CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath));
    }

    @Test
    public void testMinorVacuumWithUnexpectedUserValue() throws SQLException
    {
        hetuServer.execute("CREATE TABLE mytestdb.mytesttable (a int, b int)");
        hetuServer.execute("INSERT INTO mytestdb.mytesttable VALUES (1, 2)");
        hetuServer.execute("INSERT INTO mytestdb.mytesttable VALUES (2, 4)");
        hetuServer.execute("INSERT INTO mytestdb.mytesttable VALUES (3, 6)");
        hetuServer.execute("INSERT INTO mytestdb.mytesttable VALUES (4, 8)");

        try {
            hetuServer.execute("VACUUM TABLE mytestdb.mytesttable");
            assertEquals(FileFactory.isFileExist(storePath +
                    "/carbon.store/mytestdb/mytesttable/Fact/Part0/Segment_0.1", false), true);
        } catch (IOException e) {
            hetuServer.execute("DROP TABLE if exists mytestdb.mytesttable");
            e.printStackTrace();
        }

        hetuServer.execute("DROP TABLE if exists mytestdb.mytesttable");
    }

}
