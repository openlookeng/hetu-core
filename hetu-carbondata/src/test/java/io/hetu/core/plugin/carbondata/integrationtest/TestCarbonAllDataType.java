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
import io.prestosql.hive.$internal.au.com.bytecode.opencsv.CSVReader;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCarbonAllDataType
{
    private final Logger logger = LogServiceFactory.getLogService(TestCarbonAllDataType.class.getCanonicalName());

    private String rootPath = new File(this.getClass().getResource("/").getPath() + "../..")
            .getCanonicalPath();

    private String storePath = rootPath + "/target/store";
    private String systemPath = rootPath + "/target/system";
    private HetuTestServer hetuServer = new HetuTestServer();
    private String carbonStoreLocation = storePath + "/carbon.store";

    public TestCarbonAllDataType() throws Exception
    {
    }

    @BeforeClass
    public void setup() throws Exception
    {
        logger.info("Setup begin: " + this.getClass().getSimpleName());
        String dataPath = rootPath + "/src/test/resources/alldatatype.csv";

        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "HetuTest");

        Map<String, String> map = new HashMap<String, String>();
        map.put("hive.metastore", "file");
        map.put("hive.allow-drop-table", "true");
        map.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        map.put("carbondata.store-location", "file://" + carbonStoreLocation);
        map.put("carbondata.minor-compaction-seg-count", "4");
        map.put("carbondata.major-compaction-seg-size", "1");

        if (!FileFactory.isFileExist( storePath + "/carbon.store")) {
            FileFactory.mkdirs( storePath + "/carbon.store");
        }

        hetuServer.startServer("testdb", map);
        hetuServer.execute("drop table if exists testdb.testtable");
        hetuServer.execute("drop table if exists testdb.testtable2");
        hetuServer.execute("drop table if exists testdb.testtable3");
        hetuServer.execute("drop table if exists testdb.testtable4");
        hetuServer.execute("drop schema if exists testdb");
        hetuServer.execute("drop schema if exists default");
        hetuServer.execute("create schema testdb");
        hetuServer.execute("create schema default");

        hetuServer.execute("create table testdb.testtable(ID int, date date, country varchar, " +
                "name varchar, phonetype varchar, serialname varchar,salary double, bonus decimal(10,4), " +
                "monthlyBonus decimal(18,4), dob timestamp, shortField smallint, iscurrentemployee boolean) " +
                "with(format='CARBON') ");

        InsertIntoTableFromCSV(dataPath);
        String columnNames = "ID,date,country,name,phonetype,serialname,salary,bonus," +
                "monthlyBonus,dob,shortField,isCurrentEmployee";

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
    public void testSelectCountRows() throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 11); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectCountRowsWithDistinct() throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(DISTINCT ID) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 9); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testSelectCountRows", "testSelectCountRowsWithDistinct"})
    public void testInsertRow() throws SQLException
    {
        hetuServer.execute("INSERT INTO testdb.testtable VALUES (10, current_date , 'china' , 'KASHYAP', " +
                "'phone706', 'ASD86717', 15008.00,500.414,11.655, " +
                "timestamp '2001-08-29 13:09:03',smallint '12',true)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 12); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable WHERE name='KASHYAP' ");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testInsertRow"})
    public void testInsertMultiRow() throws SQLException
    {
        hetuServer.execute("INSERT INTO testdb.testtable VALUES (11, current_date , 'china' , 'RAGHU', 'phone706', 'ASD86717', 15008.00,500.414,11.655, timestamp '2001-08-29 13:09:03',smallint '12',true), " +
                "(12, current_date , 'canada' , 'ABHI', 'phone706', 'ASD86717', 15008.00,500.414,11.655, timestamp '2001-08-29 13:09:03',smallint '12',true), " +
                "(13, current_date , 'USA' , 'RAJEEV', 'phone706', 'ASD86717', 15008.00,500.414,11.655, timestamp '2001-08-29 13:09:03',smallint '12',true)," +
                "(14, current_date , 'austrailia' , 'AMAN', 'phone706', 'ASD86717', 15008.00,500.414,11.655, timestamp '2001-08-29 13:09:03',smallint '12',true) ");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 16); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable WHERE name in ('KASHYAP', 'RAGHU', 'ABHI', 'RAJEEV', 'AMAN')");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 5); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testInsertRow"})
    public void testUpdateOneRow() throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 16); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());

        // Update existing and update new
        hetuServer.execute("UPDATE testdb.testtable SET name='NITIN' WHERE id=6");

        actualResult = hetuServer.executeQuery("SELECT * FROM testdb.testtable");
        System.out.println("RESULT: " + actualResult.toString());

        actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 16); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable WHERE name='NITIN' ");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testUpdateOneRow"})
    public void testUpdateMultipleRow() throws SQLException
    {
        //  Update existing and update new
        hetuServer.execute("UPDATE testdb.testtable SET country='INDIA' WHERE id IN (6, 10)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 16); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable WHERE country='INDIA' ");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 2); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectTypeWithOrder() throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.testtable ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "anubhav");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "geetika");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "jatin");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "jitesh");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "liang");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "prince");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "ravindra");    }});
            add(new TreeMap<String, Object>() {{    put("NAME", "sahil");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", null);          }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectFilterWithGtExpr() throws SQLException
    {
        List<Map<String, Object>> actualResult =
                hetuServer.executeQuery("SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM testdb.testtable " +
                        "WHERE BONUS>1234 AND ID>2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
                        "BONUS ORDER BY ID");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new TreeMap<String, Object>() {{
                put("ID", 4);
                put("NAME", "prince");
                put("BONUS", BigDecimal.valueOf(9999.9990).setScale(4));
                put("DATE", "2015-07-26");
                put("SALARY", 15003.0);
                put("SERIALNAME", "ASD66902");
                put("COUNTRY", "china");
                put("PHONETYPE", "phone2435");
            }});

            add(new TreeMap<String, Object>() {{
                put("ID", 5);
                put("NAME", "bhavya");
                put("BONUS", BigDecimal.valueOf(5000.999).setScale(4));
                put("DATE", "2015-07-27");
                put("SALARY", 15004.0);
                put("SERIALNAME", "ASD90633");
                put("COUNTRY", "china");
                put("PHONETYPE", "phone2441");
            }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testUpdateMultipleRow"})
    public void testDeleteSingleRow() throws SQLException {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 16); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("delete from testdb.testtable where id=6");

        actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 15); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testDeleteSingleRow"})
    public void testInsertOverwriteTable() throws SQLException
    {
        hetuServer.execute("INSERT OVERWRITE testdb.testtable VALUES (12, current_date , 'china' , 'KASHYAP', " +
                "'phone706', 'ASD86717', 15008.00,500.414,11.655, " +
                "timestamp '2001-08-29 13:09:03',smallint '12',true)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT ID,COUNTRY,NAME FROM testdb.testtable");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("ID", 12);
                put("COUNTRY", "china");
                put("NAME", "KASHYAP");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testInsertOverwriteTable"})
    public void testInsertOverwriteEmptyTable() throws SQLException
    {
        hetuServer.execute("DELETE FROM testdb.testtable");
        hetuServer.execute("INSERT OVERWRITE testdb.testtable VALUES (2, current_date , 'india' , 'Jacob', " +
                "'phone754', 'ASD8643', 15008.00,500.414,11.655, " +
                "timestamp '2001-08-29 13:09:03',smallint '12',true)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT ID,COUNTRY,NAME FROM testdb.testtable");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("ID", 2);
                put("COUNTRY", "india");
                put("NAME", "Jacob");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test(dependsOnMethods = {"testInsertOverwriteEmptyTable"})
    public void testInsertExistingPartitionsOverwriteEmptyTable() throws SQLException
    {
        hetuServer.execute("INSERT OVERWRITE testdb.testtable VALUES (12, current_date , 'china' , 'KASHYAP', " +
                "'phone706', 'ASD86717', 15008.00,500.414,11.655, " +
                "timestamp '2001-08-29 13:09:03',smallint '12',true)");

        hetuServer.execute("set session carbondata.insert_existing_partitions_behavior = 'overwrite'");
        hetuServer.execute("INSERT INTO testdb.testtable VALUES (2, current_date , 'india' , 'Jacob', " +
                "'phone754', 'ASD8643', 15008.00,500.414,11.655, " +
                "timestamp '2001-08-29 13:09:03',smallint '12',true)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(*) AS RESULT FROM testdb.testtable");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT ID,COUNTRY,NAME FROM testdb.testtable");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("ID", 2);
                put("COUNTRY", "india");
                put("NAME", "Jacob");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testCreateTable() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (10, 11)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable2");
    }


    @Test
    public void testCreateTableLocation() throws SQLException
    {
        String location = "'" + "file:///" + storePath + "/carbon.store" + "')" ;
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int) with(format='CARBON', location = " + location);
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (10, 11)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable2");
    }

    @Test
    public void testCreateTableWrongOrderPartitionBy() throws SQLException
    {
        try {
            hetuServer.execute("CREATE TABLE testdb.testtable2 (a int, b int , c int , d int ) WITH (partitioned_by = ARRAY['c', 'a'])");
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            //same order as the table properties
            Boolean ret = e.getMessage().contains("same order as the table properties");
            assertEquals("true", ret.toString());
            return;
        }
        hetuServer.execute("drop table testdb.testtable2");
        assertEquals("true", "false");
    }

    @Test
    public void testCreateTableAs() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (20, 11)");
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (30, 11)");
        hetuServer.execute("CREATE TABLE testdb.testtable3 as select a, b from  testdb.testtable2 ");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable3");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 3); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable2");
        hetuServer.execute("drop table testdb.testtable3");
    }
    @Test
    public void testCreateTableAsMultiRow() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable3 as " +
                "select ID,date,country,name,phonetype,serialname,salary,bonus, monthlyBonus,dob,shortField,isCurrentEmployee " +
                "from  testdb.testtable");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable3");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 11); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        //hetuServer.execute("drop table testdb.testtable2");
        hetuServer.execute("drop table testdb.testtable3");
    }

    @Test
    public void testCreateTableWithLocationDisabled() throws SQLException {
        assertEquals(Assert.expectThrows(SQLException.class, () -> hetuServer.execute("CREATE TABLE carbondatacataloglocationdisabled.testdb.testtable3"
                + "(a int, b int , c int , d int ) with (location='hdfs:///user/')")).getMessage().split(":")[1]," Setting location property is not allowed");
    }

    @Test
    public void testDropTable() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtable2");
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (10, 11)");

        try {
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtable2", false), true);
            hetuServer.execute("drop table testdb.testtable2");
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtable2", false), false);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateTablePartitionByValidate() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable2 (a int, b int , c int , d int ) WITH (partitioned_by = ARRAY['c', 'd'])");

        String schemaFilePath = CarbonTablePath.getSchemaFilePath( "file://" + storePath + "/carbon.store"+ "/testdb" + "/testtable2");
        // If metadata folder exists, it is a transactional table
        CarbonFile schemaFile = FileFactory.getCarbonFile(schemaFilePath);
        boolean isTransactionalTable = schemaFile.exists();
        org.apache.carbondata.format.TableInfo tableInfo = null;
        if (isTransactionalTable) {
            //Step 2: read the metadata (tableInfo) of the table.
            ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator()
            {
                // TBase is used to read and write thrift objects.
                // TableInfo is a kind of TBase used to read and write table information.
                // TableInfo is generated by thrift,
                // see schema.thrift under format/src/main/thrift for details.
                public TBase create()
                {
                    return new org.apache.carbondata.format.TableInfo();
                }
            };
            ThriftReader thriftReader = new ThriftReader(schemaFilePath, createTBase);
            try {
                thriftReader.open();
                tableInfo = (org.apache.carbondata.format.TableInfo) thriftReader.read();
                thriftReader.close();
                List<ColumnSchema> partition_columns = tableInfo.getFact_table().getPartitionInfo().getPartition_columns();
                String [] ConfigPartiotionComumns = {"c","d"};
                String columnName;
                int i = 0;
                // validating partition columns
                for (ColumnSchema column : partition_columns) {
                    columnName =  column.getColumn_name();
                    assertEquals(ConfigPartiotionComumns[i], columnName);
                    i++;
                }
                String [] ConfigComumns = {"a", "b", "c", "d"};
                List<ColumnSchema> columnSchema = tableInfo.getFact_table().getTable_columns();
                i = 0;
                // validating columns
                for (ColumnSchema column : columnSchema) {
                    columnName =  column.getColumn_name();
                    assertEquals(ConfigComumns[i], columnName);
                    i++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Step 3: convert format level TableInfo to code level TableInfo
            SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
            // wrapperTableInfo is the code level information of a table in carbondata core,
            // different from the Thrift TableInfo.
            TableInfo wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, "testdb", "testtable2","file://" + storePath + "/carbon.store"+ "/testdb");

        }

        hetuServer.execute("drop table testdb.testtable2");
    }

    @Test
    public void testReadWriteRealDatatype()
            throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtable4");
        hetuServer.execute("CREATE TABLE testdb.testtable4(a int, b real)");
        hetuServer.execute("INSERT INTO testdb.testtable4 VALUES (10, 2),(11, 3),(12, 4)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select b as RESULT from testdb.testtable4 where a = 11");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{ put("RESULT", 3.0); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable4");
    }


    @Test(dependsOnMethods = {"testReadWriteRealDatatype"})
    public void testReadWriteTinyintDatatype()
            throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable4(a int, b tinyint)");
        hetuServer.execute("INSERT INTO testdb.testtable4 VALUES (10, tinyint '1'),(11, tinyint '2'),(12, tinyint '3')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select b as RESULT from testdb.testtable4 where a = 10");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{ put("RESULT", 49); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table if exists testdb.testtable4");
    }

    private String carbondatastorecreator(String data, int i, String[] dataTypes)
    {
        /*"ID int, date date, country varchar"
        "name varchar, phonetype varchar, serialname varchar,salary double, bonus decimal(10,4), monthlyBonus decimal(18,4), dob timestamp, shortField smallint, iscurrentemployee boolean*/
        //10, current_date , 'china' , 'KASHYAP', 'phone706', 'ASD86717', 15008.00,500.414,11.655, timestamp '2001-08-29 13:09:03',smallint '12',true
        // INSERT INTO cMt1 VALUES (1,date '2023-07-20','china','anubhav','phone197','ASD69643',5000000,1234.444,12.1234,timestamp '2016-04-14 14:00:09',smallint '1' , true);

        switch(dataTypes[i])
        {
            case "int":
            case "boolean": //true
            case "double":
            case "decimal":
                return data;

            case "date":
            {//,date '2023-07-20'
                if (data.matches("([0-9]{2})-([0-9]{2})-([0-9]{4})")) {
                    SimpleDateFormat inputFormat = new SimpleDateFormat("dd-MM-yyyy");
                    SimpleDateFormat outuptformat = new SimpleDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);

                    Date date = null;
                    try {
                        //date = inputFormat.parse("22-10-1982");
                        date = inputFormat.parse(data);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    String dateString = outuptformat.format(date);
                    //System.out.println(dateString);
                    dateString = "date '" + dateString + "'";
                    return dateString;
                }
                else  if (data.matches("([0-9]{4})/([0-9]{2})/([0-9]{2})")) {
                    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy/MM/dd");
                    SimpleDateFormat outuptformat = new SimpleDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);

                    Date date = null;
                    try {
                        //date = inputFormat.parse("22-10-1982");
                        date = inputFormat.parse(data);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    String dateString = outuptformat.format(date);
                    //System.out.println(dateString);
                    dateString = "date '" + dateString + "'";
                    return dateString;
                }
                return   "date '" + data + "'";
            }
            case "varchar":
            {//'china'
                return "'" + data + "'";
            }
            case "timestamp":
            {//timestamp '2016-04-14 14:00:09'
                Date date = null;
                String dateString = null;
                if (data.matches("([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2})/([0-9]{2})/([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("yyy-MM-dd HH/mm/ss");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    dateString = outuptformattime.format(date);
                }
                else if (data.matches("([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    dateString = outuptformattime.format(date);
                }
                else if (data.matches("([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}):([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("dd-MM-yyyy HH:mm");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    dateString = outuptformattime.format(date);
                }
                else if (data.matches("([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    dateString = outuptformattime.format(date);
                }
                else //if (!isValidFormat("yyyy-MM-dd hh:mm:ss", data))
                {
                    dateString = data;
                    //System.out.println("timestamp formate : "+ data);
                }
                //System.out.println(dateString);
                dateString = "timestamp '" + dateString + "'";
                return dateString;
            }
            case "smallint":  {
                // smallint '12'
                return "smallint '" + data + "'";
            }
        }
        return data;
    }
    private void InsertIntoTableFromCSV(String file) {
        try {
            boolean isHeader = true;
            String data = "";
            int rowCount = 0;
            String inserData = "";
            String[] dataTypes = {"int", "date", "varchar", "varchar", "varchar", "varchar", "double", "decimal", "decimal", "timestamp", "smallint", "boolean"};
            // Create an object of filereader
            // class with CSV file as a parameter.
            FileReader filereader = new FileReader(file);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader);
            String[] nextRecord;
            String covStr = null;

            // we are going to read data line by line
            int i = 0;
            while ((nextRecord = csvReader.readNext()) != null) {
                i = 0;
                data = "";
                for (String cell : nextRecord) {
                    if (i > dataTypes.length)
                        continue;

                    if (!isHeader) {
                        covStr = carbondatastorecreator(cell, i, dataTypes);
                        if (0 != i) {
                            data = data + ", ";
                        }
                        data = data + covStr;
                        i++;
                    }
                }

                if (!isHeader) {
                    data = "(" + data + ")";
                    if (0 != rowCount) {
                        inserData = inserData + ", ";
                    }
                    inserData = inserData + data;
                    //String inserData = "INSERT INTO testdb.testtable VALUES ("+ data +")";
                    //hetuServer.execute(inserData);
                    rowCount++;
                }
                isHeader = false;
            }
            data = createNullInsert(dataTypes.length);

            data = ", (" + data + ")";
            inserData = inserData + data;
            inserData = "INSERT INTO testdb.testtable VALUES" + inserData ;
            //System.out.println(inserData);
            hetuServer.execute(inserData);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private String createNullInsert(int noOfColumns)
    {
        String data = "";
        int i = 0;
        while(i < noOfColumns) {
            if (0 != i) {
                data = data + ",";
            }
            data = data + "null";
            i++;
        }
        return data;
    }

    @Test
    public void testCreateTableValidateExistingPath() throws SQLException
    {
        try {
            hetuServer.execute("drop table if exists testdb.testtable4");
            FileFactory.createDirectoryAndSetPermission(storePath  + "/carbon.store/testdb/testtable4" , null);
            try {
                hetuServer.execute("CREATE TABLE testdb.testtable4(a int, b int) with(format='CARBON') ");
            }
            catch (Exception e) {
                Boolean ret = e.getMessage().contains("Target directory for table 'testdb.testtable4' already exists:");
                assertEquals("true", ret.toString());
                CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath  + "/carbon.store/testdb/testtable4"));
                return;
            }
            hetuServer.execute("drop table if exists testdb.testtable4");
            // in error case it will come here
            assertEquals("CREATE TABLE testdb.testtable4 should throw error", "CREATE TABLE testdb.testtable4 success");

        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testVacuumNonPartitionedTable() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.mytesttable (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (1, 2)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (2, 4)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (3, 6)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (4, 8)");

        try {
            hetuServer.execute("VACUUM TABLE testdb.mytesttable");
            assertEquals(FileFactory.isFileExist(storePath +
                    "/carbon.store/testdb/mytesttable/Fact/Part0/Segment_0.1", false), true);
        } catch (IOException e) {
            hetuServer.execute("DROP TABLE if exists testdb.mytesttable");
            e.printStackTrace();
        }

        hetuServer.execute("DROP TABLE if exists testdb.mytesttable");
    }

    @Test
    public void testDoubleVacuumNonPartitionedTable() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.mytesttable2 (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.mytesttable2 VALUES (1, 2)");
        hetuServer.execute("INSERT INTO testdb.mytesttable2 VALUES (2, 4)");
        hetuServer.execute("INSERT INTO testdb.mytesttable2 VALUES (3, 6)");
        hetuServer.execute("INSERT INTO testdb.mytesttable2 VALUES (4, 8)");

        try {
            hetuServer.execute("VACUUM TABLE testdb.mytesttable2");
            hetuServer.execute("VACUUM TABLE testdb.mytesttable2");
            assertEquals(FileFactory.isFileExist(storePath +
                    "/carbon.store/testdb/mytesttable2/Fact/Part0/Segment_0.1", false), true);
        } catch (IOException e) {
            hetuServer.execute("DROP if exists TABLE testdb.mytesttable2");
            e.printStackTrace();
        }

        hetuServer.execute("DROP TABLE if exists testdb.mytesttable2");
    }

    @Test
    public void testVacuumRollback() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.myECTable (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.myECTable VALUES (1, 2)");
        hetuServer.execute("INSERT INTO testdb.myECTable VALUES (2, 4)");
        hetuServer.execute("INSERT INTO testdb.myECTable VALUES (3, 6)");
        hetuServer.execute("INSERT INTO testdb.myECTable VALUES (4, 8)");

        try {
            CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath +
                    "/carbon.store/testdb/myECTable"));
            try {
                hetuServer.execute("VACUUM TABLE testdb.myECTable");
            }
            catch (Exception e) {
                Boolean ret = e.getMessage().contains("Failed while reading metadata of the table");
                assertEquals("true", ret.toString());
                CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath +
                        "/carbon.store/testdb/myECTable"));
                return;
            }

        }
        catch (IOException | InterruptedException e) {
            hetuServer.execute("DROP TABLE if exists testdb.myECTable");
            e.printStackTrace();
        }

        hetuServer.execute("DROP TABLE if exists testdb.myECTable");
    }

    @Test
    public void testCreateDropTableDifferentLocation() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (10, 11)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable2");

        // creating table with same name
        try {
            if (!FileFactory.isFileExist( storePath + "/carbon.store/mytestDb/")) {
                FileFactory.mkdirs( storePath + "/carbon.store/mytestDb");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String location = "'" + "file:///" + storePath + "/carbon.store/mytestDb" + "')" ;
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int) with(format='CARBON', location = " + location);
        hetuServer.execute("INSERT INTO testdb.testtable2 VALUES (10, 11)");
        actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");

        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable2");
    }

    @Test
    public void testCheckingCarbonCacheUpdate() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.testtable2(a int, b int)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 0); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());

        File OriginalFile = new File(storePath + "/carbon.store/testdb/testtable2/Metadata/schema");
        // create the destination file object
        File dest = new File(storePath + "/carbon.store/testdb/testtable2/schema_backup");
        // check if the file can be renamed
        // to the abstract path name
        if (OriginalFile.renameTo(dest)) {
            // display that the file is renamed
            // to the abstract path name
            System.out.println("File is renamed");
        }
        else {
            // display that the file cannot be renamed
            // to the abstract path name
            System.out.println("File cannot be renamed");
        }
        try {
            hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");
        }
        catch (SQLException s) {
            Boolean ret = s.getMessage().contains("Failed while reading metadata of the table");
            assertEquals("true", ret.toString());
        }
        if (dest.renameTo(OriginalFile)) {
            System.out.println("File is renamed");
        }
        else {
            System.out.println("File cannot be renamed");
        }
        actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtable2");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 0); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtable2");
    }

    @Test
    public void validateMetadataEntriesAfterInsert() throws SQLException, IOException
    {
        String tableName = "testtablestatus";
        hetuServer.execute(String.format("CREATE TABLE testdb.%s (a int, b int)", tableName));
        hetuServer.execute(String.format("INSERT INTO testdb.%s VALUES(1, 2)", tableName));

        // Verify number of segment files inside Metadata Folder
        String tablePath = carbonStoreLocation + "/" + "testdb" + "/" + tableName;
        String segmentDir = tablePath + "/Metadata/segments";
        File folder = new File(segmentDir);
        assertEquals(folder.listFiles().length, 1);

        // Segment file entry should be inside table status file
        String tableStatusFilePath = tablePath + "/Metadata/tablestatus";
        String content = new String(Files.readAllBytes(Paths.get(tableStatusFilePath)));
        assertTrue(content.contains(folder.listFiles()[0].getName()));

        hetuServer.execute("DROP TABLE testdb.testTableStatus");
    }

    @Test
    public void testShowCreateTable() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.showcreatetable (a int, b int)");
        try {
            hetuServer.execute("SHOW CREATE TABLE  testdb.showcreatetable");
        }
        catch (RuntimeException e){
            hetuServer.execute("DROP TABLE testdb.showcreatetable");
            Assert.fail("Failed while executing show create table");
        }
        hetuServer.execute("DROP TABLE IF EXISTS testdb.showcreatetable");
    }
}
