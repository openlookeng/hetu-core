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

import io.hetu.core.plugin.carbondata.CarbondataMetadata;
import io.hetu.core.plugin.carbondata.server.HetuTestServer;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestCarbondataAutoCleanup
{
    private final Logger logger = LogServiceFactory.getLogService(TestCarbondataAutoCleanup.class.getCanonicalName());

    private String rootPath = new File(this.getClass().getResource("/").getPath() + "../..")
            .getCanonicalPath();

    private String storePath = rootPath + "/target/store_autocleanup";
    private String systemPath = rootPath + "/target/system";
    private HetuTestServer hetuServer = new HetuTestServer();
    private String carbonStoreLocation = storePath + "/carbon.store";

    public TestCarbondataAutoCleanup() throws Exception
    {
    }

    @BeforeClass
    public void setup() throws Exception
    {
        logger.info("Setup begin: " + this.getClass().getSimpleName());
        String dataPath = rootPath + "/src/test/resources/alldatatype.csv";

        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "HetuTest");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "0");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS, "0");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT, "1");

        Map<String, String> map = new HashMap<String, String>();
        map.put("hive.metastore", "file");
        map.put("hive.allow-drop-table", "true");
        map.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        map.put("carbondata.store-location", "file://" + carbonStoreLocation);
        map.put("carbondata.minor-vacuum-seg-count", "4");
        map.put("carbondata.major-vacuum-seg-size", "1");

        if (!FileFactory.isFileExist(storePath + "/carbon.store")) {
            FileFactory.mkdirs(storePath + "/carbon.store");
        }

        hetuServer.startServer("testdb", map);
        hetuServer.execute("drop table if exists testdb.testtableautocleanup1");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup2");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup3");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup4");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup5");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup6");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup7");
        hetuServer.execute("drop table if exists testdb.testtableautocleanup8");
        hetuServer.execute("drop schema if exists testdb");
        hetuServer.execute("drop schema if exists default");
        hetuServer.execute("create schema testdb");
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
    public void testAutoCleanupInUpdate() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup1");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup1 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup1 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup1 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup1 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup1 VALUES (130, 411)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup1 VALUES (130, 511)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup1 AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup1/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);
        hetuServer.execute("UPDATE testdb.testtableautocleanup1 SET a=232 WHERE b=511");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup1/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup1/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup1/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup1/Fact/Part0/Segment_3", false), false);
        }
        catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup1");
    }

    @Test
    public void testAutoCleanupInVacuum() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup2");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup2 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup2 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup2 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup2 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup2 VALUES (130, 411)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup2 AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup2/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("vacuum table testdb.testtableautocleanup2 AND WAIT");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup2/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup2/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup2/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup2/Fact/Part0/Segment_3", false), false);
        } catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup2");
    }

    @Test
    public void testAutoCleanupInDelete() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup3");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup3 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (130, 411)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (130, 511)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup3 AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup3/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("DELETE FROM testdb.testtableautocleanup3 WHERE a=130");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_3", false), false);
        } catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup3");
    }

    @Test
    public void testAutoCleanupInDeleteWithPushdown() throws SQLException
    {
        try {
            hetuServer.execute("set session carbondata.orc_predicate_pushdown_enabled = true");
            hetuServer.execute("drop table if exists testdb.testtableautocleanup3");
            hetuServer.execute("CREATE TABLE testdb.testtableautocleanup3 (a int, b int)");
            hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (10, 11)");
            hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (110, 211)");
            hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (120, 311)");
            hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (130, 411)");
            hetuServer.execute("INSERT INTO testdb.testtableautocleanup3 VALUES (130, 511)");
            hetuServer.execute("vacuum table testdb.testtableautocleanup3 AND WAIT");

            reduceModificationOrdeletionTimesStamp(storePath + "/carbon.store/testdb/testtableautocleanup3/Metadata");
            CarbondataMetadata.enableTracingCleanupTask(true);

            hetuServer.execute("DELETE FROM testdb.testtableautocleanup3 WHERE a=130");
            try {
                CarbondataMetadata.waitForSubmittedTasksFinish();
                assertEquals(FileFactory.isFileExist(storePath + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_0", false), false);
                assertEquals(FileFactory.isFileExist(storePath + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_1", false), false);
                assertEquals(FileFactory.isFileExist(storePath + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_2", false), false);
                assertEquals(FileFactory.isFileExist(storePath + "/carbon.store/testdb/testtableautocleanup3/Fact/Part0/Segment_3", false), false);
            } catch (IOException exception) {

            }
        }
        finally {
            CarbondataMetadata.enableTracingCleanupTask(false);
            hetuServer.execute("drop table testdb.testtableautocleanup3");
            hetuServer.execute("set session carbondata.orc_predicate_pushdown_enabled = false");
        }
    }

    @Test
    public void testAutoCleanupInInsert() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup4");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup4 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup4 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup4 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup4 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup4 VALUES (130, 411)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup4 VALUES (130, 511)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup4 AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup4/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("INSERT INTO testdb.testtableautocleanup4 VALUES (130, 511)");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup4/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup4/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup4/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup4/Fact/Part0/Segment_3", false), false);
        } catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup4");
    }

    @Test
    public void testFullAutoCleanupInUpdate() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup5");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup5 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup5 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup5 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup5 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup5 VALUES (130, 411)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup5 VALUES (130, 511)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup5 AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup5/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("UPDATE testdb.testtableautocleanup5 SET a=232 WHERE b=511");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup5/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup5/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup5/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup5/Fact/Part0/Segment_3", false), false);
        }
        catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup5");
    }

    @Test
    public void testFullAutoCleanupInVacuum() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup6");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup6 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup6 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup6 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup6 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup6 VALUES (130, 411)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup6 FULL AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup6/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("vacuum table testdb.testtableautocleanup6 FULL AND WAIT");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup6/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup6/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup6/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup6/Fact/Part0/Segment_3", false), false);
        } catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup6");
    }

    @Test
    public void testFullAutoCleanupInDelete() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup7");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup7 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup7 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup7 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup7 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup7 VALUES (130, 411)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup7 VALUES (130, 511)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup7 FULL AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup7/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("DELETE FROM testdb.testtableautocleanup7 WHERE a=130");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup7/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup7/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup7/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup7/Fact/Part0/Segment_3", false), false);
        } catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup7");
    }

    @Test
    public void testFullAutoCleanupInInsert() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtableautocleanup8");
        hetuServer.execute("CREATE TABLE testdb.testtableautocleanup8 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup8 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup8 VALUES (110, 211)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup8 VALUES (120, 311)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup8 VALUES (130, 411)");
        hetuServer.execute("INSERT INTO testdb.testtableautocleanup8 VALUES (130, 511)");
        hetuServer.execute("vacuum table testdb.testtableautocleanup8 FULL AND WAIT");

        reduceModificationOrdeletionTimesStamp(storePath  + "/carbon.store/testdb/testtableautocleanup8/Metadata");
        CarbondataMetadata.enableTracingCleanupTask(true);

        hetuServer.execute("INSERT INTO testdb.testtableautocleanup8 VALUES (130, 511)");
        try {
            CarbondataMetadata.waitForSubmittedTasksFinish();
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup8/Fact/Part0/Segment_0", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup8/Fact/Part0/Segment_1", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup8/Fact/Part0/Segment_2", false), false);
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtableautocleanup8/Fact/Part0/Segment_3", false), false);
        } catch (IOException exception) {

        }

        CarbondataMetadata.enableTracingCleanupTask(false);
        hetuServer.execute("drop table testdb.testtableautocleanup8");
    }

    private void reduceModificationOrdeletionTimesStamp(String tableMetadatPath)
    {
        LoadMetadataDetails[] metadataDetails =
                SegmentStatusManager.readLoadMetadata(tableMetadatPath);
        for (LoadMetadataDetails oneLoad : metadataDetails) {
            long deletionTime = oneLoad.getModificationOrdeletionTimesStamp();

            String modificationOrdeletionTimesStamp = Long.toString(deletionTime);
            deletionTime -= 70000L;
            String replace = Long.toString(deletionTime);
            System.out.println("replace" + replace);

            Path path = Paths.get(tableMetadatPath + "/tablestatus");
            Charset charset = StandardCharsets.UTF_8;

            String content = null;
            try {
                content = new String(Files.readAllBytes(path), charset);

                content = content.replaceFirst(modificationOrdeletionTimesStamp, replace);
                Files.write(path, content.getBytes(charset));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
