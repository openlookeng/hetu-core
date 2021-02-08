/*
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

import io.hetu.core.plugin.carbondata.CarbondataAutoVacuumThread;
import io.hetu.core.plugin.carbondata.server.HetuTestServer;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;

public class TestCarbonAutoVacuum
{
    private final Logger logger = LogServiceFactory.getLogService(TestCarbonAllDataType.class.getCanonicalName());

    private final String rootPath = new File(this.getClass().getResource("/").getPath() + "../..").getCanonicalPath();

    private final String storePath = rootPath + "/target/store_autovacuum";
    private final HetuTestServer hetuServer = new HetuTestServer();

    public TestCarbonAutoVacuum() throws Exception {
    }


    @BeforeClass
    public void setup() throws Exception
    {
        logger.info("Setup begin: " + this.getClass().getSimpleName());

        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "HetuTest");

        Map<String, String> map = new HashMap<>();
        map.put("hive.metastore", "file");
        map.put("hive.allow-drop-table", "true");
        map.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        map.put("carbondata.store-location", "file://" + storePath + "/carbon.store");
        map.put("carbondata.minor-vacuum-seg-count", "4");
        map.put("carbondata.major-vacuum-seg-size", "1");
        map.put("carbondata.auto-vacuum-enabled", "true");

        if (!FileFactory.isFileExist( storePath + "/carbon.store")) {
            FileFactory.mkdirs( storePath + "/carbon.store");
        }
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put("auto-vacuum.enabled", "true");
        configProperties.put("auto-vacuum.scan.interval", "15m");
        configProperties.put("auto-vacuum.scan.threads", "3");

        hetuServer.startServer("carbontestdb", map, configProperties);
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable1");
        hetuServer.execute("drop table if exists carbontestdb2.autovacuumtable2");
        hetuServer.execute("drop table if exists carbontestdb3.autovacuumtable3");
        hetuServer.execute("drop table if exists carbontestdb4.autovacuumtable4");

        hetuServer.execute("drop schema if exists carbontestdb1");
        hetuServer.execute("drop schema if exists carbontestdb2");
        hetuServer.execute("drop schema if exists carbontestdb3");
        hetuServer.execute("drop schema if exists carbontestdb4");

        hetuServer.execute("create schema carbontestdb1");
        hetuServer.execute("create schema carbontestdb2");
        hetuServer.execute("create schema carbontestdb3");
        hetuServer.execute("create schema carbontestdb4");

    }

    @AfterClass
    public void tearDown() throws SQLException, IOException, InterruptedException
    {
        //hetuServer.execute("drop table if exists hive.default.demotable");
        logger.info("TearDown begin: " + this.getClass().getSimpleName());
        hetuServer.stopServer();
        CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath));
    }

    @Test
    public void testAutoVacuum() throws SQLException {
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable1");
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable2");
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable3");
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable4");

        hetuServer.execute("CREATE TABLE carbontestdb1.autovacuumtable1(a int, b int)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable1 VALUES (10, 11)");

        hetuServer.execute("CREATE TABLE carbontestdb1.autovacuumtable2(a int, b int)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable2 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable2 VALUES (20, 11)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable2 VALUES (30, 11)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable2 VALUES (30, 11)");

        hetuServer.execute("CREATE TABLE carbontestdb1.autovacuumtable3 as select a, b from  carbontestdb1.autovacuumtable2 ");

        hetuServer.execute("CREATE TABLE carbontestdb1.autovacuumtable4(a int, b int)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable4 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable4 VALUES (20, 11)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable4 VALUES (30, 12)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable4 VALUES (40, 13)");
        CatalogManager catalogManager = hetuServer.getCatalog();
        List<Catalog> catalogs = catalogManager.getCatalogs();
        Catalog catalog = null;
        for (Catalog catalog1 : catalogs) {
            if (catalog1.getConnectorCatalogName().getCatalogName().equals("carbondata")) {
                catalog = catalog1;
                break;
            }
        }

        Connector connector;
        ConnectorMetadata connectorMetadata = null;
        List<ConnectorVacuumTableInfo> tables;
        List<String> tablesNames = new ArrayList<>();

        try {
            CarbondataAutoVacuumThread.enableTracingVacuumTask(true);
            connector = catalog.getConnector(catalog.getConnectorCatalogName());
            connectorMetadata = connector.getConnectorMetadata();
            connectorMetadata.getTablesForVacuum();
        } catch (Exception e) {

        }

        CarbondataAutoVacuumThread.waitForSubmittedVacuumTasksFinish();
        CarbondataAutoVacuumThread.enableTracingVacuumTask(false);
        tables = connectorMetadata.getTablesForVacuum();
        if (tables != null && 0 != tables.size()) {
            for (ConnectorVacuumTableInfo vacuumTable : tables) {
                tablesNames.add(vacuumTable.getSchemaTableName());
            }
        }

        assertTrue( tablesNames.contains("carbontestdb1.autovacuumtable4"));
        assertTrue( tablesNames.contains("carbontestdb1.autovacuumtable2"));
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable1");
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable2");
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable3");
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable4");
    }

    @Test
    public void testAutoVacuumDiffDB() throws SQLException, InterruptedException {
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable1");
        hetuServer.execute("drop table if exists carbontestdb2.autovacuumtable2");
        hetuServer.execute("drop table if exists carbontestdb3.autovacuumtable3");
        hetuServer.execute("drop table if exists carbontestdb4.autovacuumtable4");

        hetuServer.execute("CREATE TABLE carbontestdb1.autovacuumtable1(a int, b int)");
        hetuServer.execute("INSERT INTO carbontestdb1.autovacuumtable1 VALUES (10, 11)");

        hetuServer.execute("CREATE TABLE carbontestdb2.autovacuumtable2(a int, b int)");
        hetuServer.execute("INSERT INTO carbontestdb2.autovacuumtable2 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO carbontestdb2.autovacuumtable2 VALUES (20, 11)");
        hetuServer.execute("INSERT INTO carbontestdb2.autovacuumtable2 VALUES (30, 11)");
        hetuServer.execute("INSERT INTO carbontestdb2.autovacuumtable2 VALUES (30, 11)");

        hetuServer.execute("CREATE TABLE carbontestdb3.autovacuumtable3 as select a, b from  carbontestdb2.autovacuumtable2 ");

        hetuServer.execute("CREATE TABLE carbontestdb4.autovacuumtable4(a int, b int)");
        hetuServer.execute("INSERT INTO carbontestdb4.autovacuumtable4 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO carbontestdb4.autovacuumtable4 VALUES (20, 11)");
        hetuServer.execute("INSERT INTO carbontestdb4.autovacuumtable4 VALUES (30, 12)");
        hetuServer.execute("INSERT INTO carbontestdb4.autovacuumtable4 VALUES (40, 13)");

        CatalogManager catalogManager = hetuServer.getCatalog();
        List<Catalog> catalogs = catalogManager.getCatalogs();
        Catalog catalog = null;
        for (Catalog catalog1 : catalogs) {
            if (catalog1.getConnectorCatalogName().getCatalogName().equals("carbondata")) {
                catalog = catalog1;
                break;
            }
        }

        Connector connector;
        ConnectorMetadata connectorMetadata = null;
        List<ConnectorVacuumTableInfo> tables = null;
        List<String> tablesNames = new ArrayList<>();

        try {

            CarbondataAutoVacuumThread.enableTracingVacuumTask(true);
            connector = catalog.getConnector(catalog.getConnectorCatalogName());
            connectorMetadata = connector.getConnectorMetadata();
            connectorMetadata.getTablesForVacuum();
        } catch (Exception e) {

        }

        CarbondataAutoVacuumThread.waitForSubmittedVacuumTasksFinish();
        CarbondataAutoVacuumThread.enableTracingVacuumTask(false);
        tables = connectorMetadata.getTablesForVacuum();
        if (tables != null && 0 != tables.size()) {
            for (ConnectorVacuumTableInfo vacuumTable : tables) {
                tablesNames.add(vacuumTable.getSchemaTableName());
            }
        }
        assertTrue( tablesNames.contains("carbontestdb4.autovacuumtable4"));
        assertTrue( tablesNames.contains("carbontestdb2.autovacuumtable2"));
        hetuServer.execute("drop table if exists carbontestdb1.autovacuumtable1");
        hetuServer.execute("drop table if exists carbontestdb2.autovacuumtable2");
        hetuServer.execute("drop table if exists carbontestdb3.autovacuumtable3");
        hetuServer.execute("drop table if exists carbontestdb4.autovacuumtable4");
    }
}
