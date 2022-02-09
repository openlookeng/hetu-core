/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.gson.Gson;
import io.hetu.core.plugin.carbondata.server.HetuTestServer;
import io.prestosql.hive.$internal.au.com.bytecode.opencsv.CSVReader;
import io.prestosql.spi.PrestoException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME, "0");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS, "0");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT, "1");

        Map<String, String> map = new HashMap<String, String>();
        map.put("hive.metastore", "file");
        map.put("hive.allow-drop-table", "true");
        map.put("hive.allow-drop-column", "true");
        map.put("hive.allow-add-column", "true");
        map.put("hive.allow-rename-column", "true");
        map.put("hive.metastore.catalog.dir", "file://" + storePath + "/hive.store");
        map.put("carbondata.store-location", "file://" + carbonStoreLocation);
        map.put("carbondata.minor-vacuum-seg-count", "4");
        map.put("carbondata.major-vacuum-seg-size", "1");

        if (!FileFactory.isFileExist(storePath + "/carbon.store")) {
            FileFactory.mkdirs(storePath + "/carbon.store");
        }

        hetuServer.startServer("testdb", map);
        hetuServer.execute("drop table if exists testdb.testtable");
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
            add(new HashMap<String, Object>() {{put("RESULT", 11); }});
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

    @Test
    public void testSelectSum()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT SUM(ID) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 54);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectAvgDistinct()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT AVG(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 5.0);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectMinDistinct()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT MIN(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 1);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectMaxDistinct()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT MAX(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 9);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectCountDistinctDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 10);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectCountDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT COUNT(BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 10);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectSumDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT SUM(BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 20774.6475);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectSumDistinctDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT SUM(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 20774.6475);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectAvgDistinctDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT AVG(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 2077.4648);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectMinDistinctDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT MIN(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.4140).setScale(4));
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectMaxDistinctDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT MAX(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(9999.9990).setScale(4));
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectDecimalOrderBy()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT DISTINCT BONUS AS RESULT FROM TESTDB.TESTTABLE ORDER BY BONUS LIMIT 3");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.414).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.59).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.88).setScale(4));
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectStringOrderBy()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME AS RESULT FROM TESTDB.TESTTABLE ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "akash");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "anubhav");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "bhavya");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "geetika");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "jatin");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "jitesh");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "liang");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "prince");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "ravindra");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "sahil");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", null);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectDateOrderBy()
            throws SQLException, ParseException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT DATE AS RESULT FROM TESTDB.TESTTABLE WHERE id < 10 ORDER BY DATE");
        DateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-18");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-18");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-23");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-24");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-25");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-26");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-27");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-28");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-29");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "2015-07-30");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testFilterWithLessThanEqualTo()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE" +
                " WHERE BONUS <= 1234.444 AND ID > 2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY, " +
                "BONUS ORDER BY ID LIMIT 2");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>()
            {{
                put("ID", 3);
                put("NAME", "liang");
                put("BONUS", BigDecimal.valueOf(600.7770).setScale(4));
                put("DATE", "2015-07-25");
                put("SALARY", 15002.11);
                put("SERIALNAME", "ASD37014");
                put("COUNTRY", "china");
                put("PHONETYPE", "phone1904");
            }});

            add(new TreeMap<String, Object>()
            {{
                put("ID", 6);
                put("NAME", "akash");
                put("BONUS", BigDecimal.valueOf(500.5900).setScale(4));
                put("DATE", "2015-07-28");
                put("SALARY", 15005.0);
                put("SERIALNAME", "ASD59961");
                put("COUNTRY", "china");
                put("PHONETYPE", "phone294");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testFilterWithEqualToOnDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID AS RESULT FROM TESTDB.TESTTABLE WHERE BONUS=1234.444");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 1);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testFilterWithLessThanWithAnd()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE" +
                " WHERE BONUS>1234 AND ID<2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," + " BONUS ORDER BY ID");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>()
            {{
                put("ID", 1);
                put("NAME", "anubhav");
                put("BONUS", BigDecimal.valueOf(1234.4440).setScale(4));
                put("DATE", "2015-07-23");
                put("SALARY", 5000000.0);
                put("SERIALNAME", "ASD69643");
                put("COUNTRY", "china");
                put("PHONETYPE", "phone197");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectWithIn()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME AS RESULT from testdb.testtable WHERE PHONETYPE IN('phone1848','phone706') ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "geetika");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "jitesh");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "ravindra");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectWithNotIn()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME AS RESULT from testdb.testtable WHERE PHONETYPE NOT IN('phone1848','phone706') ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "akash");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "anubhav");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "bhavya");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "jatin");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "liang");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "prince");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "sahil");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testIsNotNullOnDate()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME AS RESULT FROM TESTDB.TESTTABLE WHERE DATE IS NOT NULL AND ID=9");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "ravindra");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "jitesh");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testIsNotNullTimestamp()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME AS RESULT FROM TESTDB.TESTTABLE WHERE DOB IS NOT NULL AND ID=9");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "ravindra");
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", "jitesh");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectShortWithOrderBy()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT DISTINCT SHORTFIELD AS RESULT from testdb.testtable ORDER BY SHORTFIELD");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 1);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 4);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 8);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 10);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 11);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 12);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 17);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 18);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", null);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectShortIsNullWithOrderBy()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID AS RESULT from testdb.testtable WHERE SHORTFIELD IS NULL ORDER BY SHORTFIELD");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", null);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectShortWithGreaterThan()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID AS RESULT from testdb.testtable WHERE SHORTFIELD>11 ORDER BY ID");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 6);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 7);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 9);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectLongDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID  AS RESULT from testdb.testtable WHERE bonus = DECIMAL '1234.5555'");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 2);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectShortDecimal()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT ID AS RESULT from testdb.testtable WHERE monthlyBonus = 15.13");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 2);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectBoolean()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT isCurrentEmployee AS RESULT FROM TESTDB.TESTTABLE WHERE ID=1");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", true);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectBooleanIsNull()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT id AS RESULT FROM TESTDB.TESTTABLE WHERE isCurrentEmployee is null");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 2);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", null);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectBooleanIsNotNull()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT id AS RESULT FROM TESTDB.TESTTABLE WHERE isCurrentEmployee is NOT null AND ID>8 ORDER BY ID");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 9);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 9);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testShowSchemas()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SHOW SCHEMAS");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("Schema", "default");
            }});
            add(new HashMap<String, Object>()
            {{
                put("Schema", "information_schema");
            }});
            add(new HashMap<String, Object>()
            {{
                put("Schema", "testdb");
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectWithOr()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT BONUS AS RESULT FROM TESTDB.TESTTABLE WHERE" +
                " BONUS < 600 OR BONUS > 5000 ORDER BY BONUS");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.414).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.59).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.88).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(500.99).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(5000.9990).setScale(4));
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", BigDecimal.valueOf(9999.9990).setScale(4));
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectWithOrAndAnd()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT SHORTFIELD AS RESULT FROM TESTDB.TESTTABLE WHERE" +
                " SHORTFIELD > 4 AND (SHORTFIELD < 10 or SHORTFIELD > 15) ORDER BY SHORTFIELD");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 8);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 17);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 18);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectWithOrAndMultipleAnds()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT SHORTFIELD AS RESULT FROM TESTDB.TESTTABLE WHERE" +
                " (SHORTFIELD > 1 AND SHORTFIELD < 5) OR (SHORTFIELD > 10 AND SHORTFIELD < 15) ORDER BY SHORTFIELD");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 4);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 11);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 12);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
    }

    @Test
    public void testSelectWithOrAndAndOnDiffColumn()
            throws SQLException
    {
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT SHORTFIELD AS RESULT FROM TESTDB.TESTTABLE WHERE" +
                " ID < 7 AND (SHORTFIELD < 5 OR SHORTFIELD > 15) ORDER BY SHORTFIELD");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 4);
            }});
            add(new HashMap<String, Object>()
            {{
                put("RESULT", 18);
            }});
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

    @Test
    public void testAlterAddColumn() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.addcolumn(NAME varchar(7), AGE int)");
        hetuServer.execute("INSERT INTO testdb.addcolumn VALUES ('Raj', 11)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT AGE FROM testdb.addcolumn");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("AGE", 11); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("ALTER TABLE testdb.addcolumn ADD COLUMN COUNTRY varchar(7)");

        TableInfo tableInfo = getTableInfoFromSchemaFile("addcolumn");
        assertEquals(tableInfo.getFactTable().getListOfColumns().stream().filter(s -> s.getColumnName().equals("country")).collect(Collectors.toList()).get(0).getColumnName(), "country" );
        if(tableInfo.getFactTable().getListOfColumns().stream().filter(s -> s.getColumnName().equals("country")).collect(Collectors.toList()).get(0).getColumnName().equals("country")) {
            actualResult = hetuServer.executeQuery("SELECT * FROM testdb.addcolumn");

            expectedResult = new ArrayList<Map<String, Object>>() {{
                add(new TreeMap<String, Object>() {{
                    put("age", 11);
                    put("country", null);
                    put("name", "Raj");
                }});
            }};
            assertEquals(actualResult.toString(), expectedResult.toString());
        }
        hetuServer.execute("drop table if exists testdb.addcolumn");
    }

    @Test
    public void testAlterRenameColumn() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.renamecolumn(NAME varchar(7), AGE int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.renamecolumn VALUES ('Raj', 11)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT AGE FROM testdb.renamecolumn");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("AGE", 11);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("ALTER TABLE testdb.renamecolumn RENAME COLUMN AGE TO ID");

        TableInfo tableInfo = getTableInfoFromSchemaFile("renamecolumn");
        assertEquals(tableInfo.getFactTable().getListOfColumns().stream().filter(s -> s.getColumnName().equals("id")).collect(Collectors.toList()).get(0).getColumnName(), "id" );

        if(tableInfo.getFactTable().getListOfColumns().stream().filter(s -> s.getColumnName().equals("id")).collect(Collectors.toList()).get(0).getColumnName().equals("id")) {
            actualResult = hetuServer.executeQuery("SELECT * FROM testdb.renamecolumn");

            expectedResult = new ArrayList<Map<String, Object>>() {{
                add(new TreeMap<String, Object>() {{
                    put("id", "11");
                    put("name", "Raj");
                }});
            }};
            assertEquals(actualResult.toString(), expectedResult.toString());
        }
        hetuServer.execute("drop table if exists testdb.renamecolumn");
    }

    @Test
    public void testAlterDropColumn() throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.dropcolumn(NAME varchar(7), AGE int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.dropcolumn VALUES ('Raj', 11)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT AGE FROM testdb.dropcolumn");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{
                put("AGE", 11);
            }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("ALTER TABLE testdb.dropcolumn DROP COLUMN AGE");

        actualResult = hetuServer.executeQuery("SELECT * FROM testdb.dropcolumn");

        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("name", "Raj"); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("drop table if exists testdb.dropcolumn");
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
        hetuServer.execute("drop table if exists testdb.testtablecreate");
        hetuServer.execute("CREATE TABLE testdb.testtablecreate(a int, b int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.testtablecreate VALUES (10, 11)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablecreate");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtablecreate");
    }

    @Test
    public void testReadWriteVarbinaryDatatype()
            throws SQLException
    {
        hetuServer.execute("CREATE TABLE testdb.varbinarytype(a int, b varbinary)");
        hetuServer.execute("INSERT INTO testdb.varbinarytype VALUES (10, varbinary 'b'),(11, varbinary 'c'),(12, varbinary 'd')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count(b) as RESULT from testdb.varbinarytype where a = 10");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{ put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("drop table  testdb.varbinarytype");
    }

    @Test
    public void testCreateTableLocation() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtablelocation");
        String location = "'" + "file:///" + storePath + "/carbon.store" + "')" ;
        hetuServer.execute("CREATE TABLE testdb.testtablelocation(a int, b int) with(format='CARBON', location = " + location);
        hetuServer.execute("INSERT INTO testdb.testtablelocation VALUES (10, 11)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablelocation");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtablelocation");
    }

    @Test
    public void testCreateTableAs() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.createsourcetable");
        hetuServer.execute("CREATE TABLE testdb.createsourcetable(a int, b int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.createsourcetable VALUES (10, 11)");
        hetuServer.execute("INSERT INTO testdb.createsourcetable VALUES (20, 11)");
        hetuServer.execute("INSERT INTO testdb.createsourcetable VALUES (30, 11)");
        hetuServer.execute("CREATE TABLE testdb.createtableas as select a, b from  testdb.createsourcetable ");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.createtableas");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 3); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.createsourcetable");
        hetuServer.execute("drop table testdb.createtableas");
    }
    @Test
    public void testCreateTableAsMultiRow() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.createtablemultiplerows");
        hetuServer.execute("CREATE TABLE testdb.createtablemultiplerows as " +
                "select ID,date,country,name,phonetype,serialname,salary,bonus, monthlyBonus,dob,shortField,isCurrentEmployee " +
                "from  testdb.testtable");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.createtablemultiplerows");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 11); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.createtablemultiplerows");
    }

    @Test
    public void testCreateTableWithLocationDisabled() throws SQLException {
        hetuServer.execute("drop table if exists testdb.testtable3");
        assertEquals(Assert.expectThrows(SQLException.class, () -> hetuServer.execute("CREATE TABLE carbondatacataloglocationdisabled.testdb.testtable3"
                + "(a int, b int , c int , d int ) with (location='hdfs:///user/')")).getMessage().split(":")[1]," Setting location property is not allowed");
    }

    @Test
    public void testDropTable() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtabledrop");
        hetuServer.execute("CREATE TABLE testdb.testtabledrop(a int, b int) with(format='CARBON') ");
        hetuServer.execute("INSERT INTO testdb.testtabledrop VALUES (10, 11)");

        try {
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtabledrop", false), true);
            hetuServer.execute("drop table testdb.testtabledrop");
            assertEquals(FileFactory.isFileExist(storePath  + "/carbon.store/testdb/testtabledrop", false), false);

        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void testCreateTableValidate() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtablevalidate");
        hetuServer.execute("CREATE TABLE testdb.testtablevalidate (a int, b int , c int , d int )");

        String schemaFilePath = CarbonTablePath.getSchemaFilePath( "file://" + storePath + "/carbon.store"+ "/testdb" + "/testtablevalidate");
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
                String columnName;
                int i = 0;

                String [] ConfigComumns = {"a", "b", "c", "d"};
                List<ColumnSchema> columnSchema = tableInfo.getFact_table().getTable_columns();
                // validating columns
                for (ColumnSchema column : columnSchema) {
                    columnName =  column.getColumn_name();
                    assertEquals(ConfigComumns[i], columnName);
                    i++;
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }

            // Step 3: convert format level TableInfo to code level TableInfo
            SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
            // wrapperTableInfo is the code level information of a table in carbondata core,
            // different from the Thrift TableInfo.
            TableInfo wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, "testdb", "testtablevalidate","file://" + storePath + "/carbon.store"+ "/testdb");

        }

        hetuServer.execute("drop table testdb.testtablevalidate");
    }

    @Test
    public void testReadWriteRealDatatype()
            throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtablerealdatatype");
        hetuServer.execute("CREATE TABLE testdb.testtablerealdatatype(a int, b real)");
        hetuServer.execute("INSERT INTO testdb.testtablerealdatatype VALUES (10, 2),(11, 3),(12, 4)");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select b as RESULT from testdb.testtablerealdatatype where a = 11");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{ put("RESULT", 3.0); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtablerealdatatype");
    }


    @Test(dependsOnMethods = {"testReadWriteRealDatatype"})
    public void testReadWriteTinyintDatatype()
            throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtabletinydatatype");
        hetuServer.execute("CREATE TABLE testdb.testtabletinydatatype(a int, b tinyint)");
        hetuServer.execute("INSERT INTO testdb.testtabletinydatatype VALUES (10, tinyint '1'),(11, tinyint '2'),(12, tinyint '3')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select b as RESULT from testdb.testtabletinydatatype where a = 10");

        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new HashMap<String, Object>()
            {{ put("RESULT", 49); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table if exists testdb.testtabletinydatatype");
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
                        date = inputFormat.parse(data);
                    } catch (ParseException e) {
                        logger.error(e.getMessage());
                    }
                    String dateString = outuptformat.format(date);
                    dateString = "date '" + dateString + "'";
                    return dateString;
                }
                else  if (data.matches("([0-9]{4})/([0-9]{2})/([0-9]{2})")) {
                    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy/MM/dd");
                    SimpleDateFormat outuptformat = new SimpleDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);

                    Date date = null;
                    try {
                        date = inputFormat.parse(data);
                    } catch (ParseException e) {
                        logger.error(e.getMessage());
                    }
                    String dateString = outuptformat.format(date);
                    dateString = "date '" + dateString + "'";
                    return dateString;
                }
                return   "date '" + data + "'";
            }
            case "varchar":
            {
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
                        logger.error(e.getMessage());
                    }
                    dateString = outuptformattime.format(date);
                }
                else if (data.matches("([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        logger.error(e.getMessage());
                    }
                    dateString = outuptformattime.format(date);
                }
                else if (data.matches("([0-9]{2})-([0-9]{2})-([0-9]{4}) ([0-9]{2}):([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("dd-MM-yyyy HH:mm");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        logger.error(e.getMessage());
                    }
                    dateString = outuptformattime.format(date);
                }
                else if (data.matches("([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2})")) {
                    SimpleDateFormat inputFormattime = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    SimpleDateFormat outuptformattime = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

                    try {
                        date = inputFormattime.parse(data);
                    } catch (ParseException e) {
                        logger.error(e.getMessage());
                    }
                    dateString = outuptformattime.format(date);
                }
                else
                {
                    dateString = data;
                }
                dateString = "timestamp '" + dateString + "'";
                return dateString;
            }
            case "smallint":  {
                return "smallint '" + data + "'";
            }
            default:
                break;
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
                    rowCount++;
                }
                isHeader = false;
            }
            data = createNullInsert(dataTypes.length);

            data = ", (" + data + ")";
            inserData = inserData + data;
            inserData = "INSERT INTO testdb.testtable VALUES" + inserData ;
            hetuServer.execute(inserData);
        }
        catch(Exception e) {
            logger.error(e.getMessage());
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
            hetuServer.execute("drop table if exists testdb.testtablevalidateexistingpath");
            FileFactory.createDirectoryAndSetPermission(storePath  + "/carbon.store/testdb/testtablevalidateexistingpath" , null);
            try {
                hetuServer.execute("CREATE TABLE testdb.testtablevalidateexistingpath(a int, b int) with(format='CARBON') ");
            }
            catch (Exception e) {
                Boolean ret = e.getMessage().contains("Target directory for table 'testdb.testtablevalidateexistingpath' already exists:");
                assertEquals("true", ret.toString());
                CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath  + "/carbon.store/testdb/testtablevalidateexistingpath"));
                return;
            }
            hetuServer.execute("drop table if exists testdb.testtablevalidateexistingpath");
            // in error case it will come here
            assertEquals("CREATE TABLE testdb.testtablevalidateexistingpath should throw error", "CREATE TABLE testdb.testtablevalidateexistingpath success");

        }
        catch (IOException | InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void testSegmentDelete() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.segmentdelete");
        hetuServer.execute("CREATE TABLE testdb.segmentdelete(a int, b tinyint)");
        hetuServer.execute("INSERT INTO testdb.segmentdelete VALUES (10, tinyint '1'),(11, tinyint '2'),(12, tinyint '3')");
        hetuServer.execute("INSERT INTO testdb.segmentdelete VALUES (13, tinyint '1'),(14, tinyint '2'),(15, tinyint '3')");
        hetuServer.execute("INSERT INTO testdb.segmentdelete VALUES (16, tinyint '1'),(17, tinyint '2'),(18, tinyint '3')");
        hetuServer.execute("INSERT INTO testdb.segmentdelete VALUES (19, tinyint '1'),(20, tinyint '2'),(21, tinyint '3')");
        hetuServer.execute("INSERT INTO testdb.segmentdelete VALUES (22, tinyint '1'),(23, tinyint '2'),(24, tinyint '3')");
        hetuServer.execute("INSERT INTO testdb.segmentdelete VALUES (25, tinyint '1'),(26, tinyint '2'),(27, tinyint '3')");

        // Check if marked for delete  if a single row is deleted
        hetuServer.execute("DELETE FROM testdb.segmentdelete WHERE a = 10");
        assertFalse(checkStatusFileForDeleteMarked("segmentdelete", 0, 0));

        // Check if marked for delete if whole segment is deleted
        hetuServer.execute("DELETE FROM testdb.segmentdelete WHERE a < 16");
        assertTrue(checkStatusFileForDeleteMarked("segmentdelete", 1, 1));

        // Check if marked for delete if rows of multiple segments are deleted
        hetuServer.execute("DELETE FROM testdb.segmentdelete WHERE a < 21");
        assertTrue(checkStatusFileForDeleteMarked("segmentdelete", 2, 2));
        assertFalse(checkStatusFileForDeleteMarked("segmentdelete", 2, 3));

        // Check if marked for delete after an update
        hetuServer.execute("UPDATE testdb.segmentdelete SET a=27 WHERE a = 22");
        assertFalse(checkStatusFileForDeleteMarked("segmentdelete", 3, 3));

        // Check if marked for delete after a delete after another delete and update
        hetuServer.execute("DELETE FROM testdb.segmentdelete WHERE a = 25");
        assertFalse(checkStatusFileForDeleteMarked("segmentdelete", 4, 4));
        hetuServer.execute("UPDATE testdb.segmentdelete SET a=28 WHERE a = 26");
        assertFalse(checkStatusFileForDeleteMarked("segmentdelete", 5, 4));
        hetuServer.execute("DELETE FROM testdb.segmentdelete WHERE a < 29");
        assertTrue(checkStatusFileForDeleteMarked("segmentdelete", 6, 4));

        hetuServer.execute("drop table if exists testdb.segmentdelete");
    }

    /*
        Returns true if "Marked for Delete" is present in both tableupdatestatus and tablestatus file
    */
    private boolean checkStatusFileForDeleteMarked(String tableName, int updateNumber, int segmentNumber) throws SQLException
    {
        BufferedReader reader = null;
        try {
            File dir = new File(storePath + "/carbon.store/testdb/" + tableName + "/Metadata");
            File[] tableUpdateStatusFiles = dir.listFiles((d, name) -> name.startsWith("tableupdatestatus"));
            Arrays.sort(tableUpdateStatusFiles);
            Gson gson = new Gson();
            reader = new BufferedReader(new FileReader(tableUpdateStatusFiles[updateNumber]));
            SegmentUpdateDetails[] segmentUpdateDetails = gson.fromJson(reader, SegmentUpdateDetails[].class);
            File tableStatusFile = new File(dir.getCanonicalPath() + "/tablestatus");
            reader = new BufferedReader(new FileReader(tableStatusFile));
            LoadMetadataDetails loadMetadataDetails = gson.fromJson(reader, LoadMetadataDetails[].class)[segmentNumber];
            if ((segmentUpdateDetails[0].getSegmentStatus() != null && segmentUpdateDetails[0].getSegmentStatus().toString().equals("Marked for Delete")) &&
                    loadMetadataDetails.getSegmentStatus().toString().equals("Marked for Delete")) {
                return true;
            }
        } catch (IOException e) {
            hetuServer.execute("drop table if exists testdb." + tableName);
            Assert.fail("Failed to read status files");
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
        return false;
    }

    @Test
    public void testVacuumNonPartitionedTable() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.mytesttable");
        hetuServer.execute("CREATE TABLE testdb.mytesttable (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (1, 2)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (2, 4)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (3, 6)");
        hetuServer.execute("INSERT INTO testdb.mytesttable VALUES (4, 8)");

        try {
            hetuServer.execute("VACUUM TABLE testdb.mytesttable AND WAIT");
            assertEquals(FileFactory.isFileExist(storePath +
                    "/carbon.store/testdb/mytesttable/Fact/Part0/Segment_0.1", false), true);
        } catch (IOException e) {
            hetuServer.execute("DROP TABLE if exists testdb.mytesttable");
            logger.error(e.getMessage());
        }

        hetuServer.execute("DROP TABLE if exists testdb.mytesttable");
    }

    @Test
    public void testDoubleVacuumNonPartitionedTable() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.mytesttable2");
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
        }
        catch (IOException e) {
            hetuServer.execute("DROP TABLE if exists testdb.mytesttable2");
            logger.error(e.getMessage());
        }

        hetuServer.execute("DROP TABLE if exists testdb.mytesttable2");
    }

    @Test
    public void testVacuumRollback() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.myectable");
        hetuServer.execute("CREATE TABLE testdb.myectable (a int, b int)");
        hetuServer.execute("INSERT INTO testdb.myectable VALUES (1, 2)");
        hetuServer.execute("INSERT INTO testdb.myectable VALUES (2, 4)");
        hetuServer.execute("INSERT INTO testdb.myectable VALUES (3, 6)");
        hetuServer.execute("INSERT INTO testdb.myectable VALUES (4, 8)");

        try {
            CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath +
                    "/carbon.store/testdb/myectable"));
            try {
                hetuServer.execute("VACUUM TABLE testdb.myectable");
            }
            catch (Exception e) {
                Boolean ret = e.getMessage().contains("Failed while reading metadata of the table");
                assertEquals("true", ret.toString());
                CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath +
                        "/carbon.store/testdb/myectable"));
                return;
            }

        }
        catch (IOException | InterruptedException e) {
            hetuServer.execute("DROP TABLE if exists testdb.myectable");
            logger.error(e.getMessage());
        }

        hetuServer.execute("DROP TABLE if exists testdb.myectable");
    }

    @Test
    public void testCreateDropTableDifferentLocation() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtablediffloc");
        hetuServer.execute("CREATE TABLE testdb.testtablediffloc(a int, b int)");
        hetuServer.execute("INSERT INTO testdb.testtablediffloc VALUES (10, 11)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablediffloc");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtablediffloc");

        // creating table with same name
        try {
            if (!FileFactory.isFileExist( storePath + "/carbon.store/mytestDb/")) {
                FileFactory.mkdirs( storePath + "/carbon.store/mytestDb");
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        String location = "'" + "file:///" + storePath + "/carbon.store/mytestDb" + "')" ;
        hetuServer.execute("CREATE TABLE testdb.testtablediffloc(a int, b int) with(format='CARBON', location = " + location);
        hetuServer.execute("INSERT INTO testdb.testtablediffloc VALUES (10, 11)");
        actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablediffloc");

        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 1); }});
        }};

        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtablediffloc");
    }

    @Test
    public void testCheckingCarbonCacheUpdate() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.testtablecacheupdate");
        hetuServer.execute("CREATE TABLE testdb.testtablecacheupdate(a int, b int)");
        List<Map<String, Object>> actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablecacheupdate");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 0); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());

        File OriginalFile = new File(storePath + "/carbon.store/testdb/testtablecacheupdate/Metadata/schema");
        // create the destination file object
        File dest = new File(storePath + "/carbon.store/testdb/testtablecacheupdate/schema_backup");
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
            hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablecacheupdate");
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
        actualResult = hetuServer.executeQuery("Select count (*) as RESULT from testdb.testtablecacheupdate");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("RESULT", 0); }});
        }};
        assertEquals(actualResult.toString(), expectedResult.toString());
        hetuServer.execute("drop table testdb.testtablecacheupdate");
    }

    @Test
    public void validateMetadataEntriesAfterInsert() throws SQLException, IOException
    {
        String tableName = "testtablestatus";
        hetuServer.execute("drop table if exists testdb.testtablestatus");
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
        hetuServer.execute("drop table if exists testdb.showcreatetable");
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

    @Test
    public void testFilterUnboundedVarcharDatatype() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.unboundedvarchar");
        hetuServer.execute("CREATE TABLE testdb.unboundedvarchar(name varchar)");
        hetuServer.execute("INSERT INTO testdb.unboundedvarchar VALUES('akash'),('anubhav'),('bhavya'),('amit  ')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarchar ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "anubhav");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarchar WHERE NAME='akash'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarchar WHERE NAME='amit  '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarchar WHERE NAME='amit'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("DROP TABLE testdb.unboundedvarchar");
    }

    @Test
    public void testFilterBoundedVarcharDatatype() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.boundedvarchar");
        hetuServer.execute("CREATE TABLE testdb.boundedvarchar(name varchar(6))");
        hetuServer.execute("INSERT INTO testdb.boundedvarchar VALUES('akash'),('anubav'),('bhavya'), ('amit  ')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarchar ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "anubav");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarchar WHERE NAME='akash'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarchar WHERE NAME='bhavya'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarchar WHERE NAME='amit  '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarchar WHERE NAME='amit'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarchar WHERE NAME='amit   '");
        expectedResult = new ArrayList<Map<String, Object>>() {{
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());
        try {
            hetuServer.execute("INSERT INTO testdb.boundedvarchar VALUES('akashmital')");
            Assert.fail("Exception expected");
        }
        catch(SQLException e){
            assertEquals(e.getMessage().split(":")[1], " Insert query has mismatched column types");
        }


        hetuServer.execute("DROP TABLE testdb.boundedvarchar");
    }

    @Test
    public void testFilterUnboundedCharDatatype() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.unboundedchar");
        hetuServer.execute("CREATE TABLE testdb.unboundedchar(name char)");
        hetuServer.execute("INSERT INTO testdb.unboundedchar VALUES('a'),('b'),('c'),('d')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedchar ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "a");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "b");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "c");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "d");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedchar WHERE NAME='a'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "a");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedchar WHERE NAME='a '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "a");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        try {
            hetuServer.execute("INSERT INTO testdb.unboundedchar VALUES('a ')");
            Assert.fail("Exception expected");
        }
        catch(SQLException e){
            assertEquals(e.getMessage().split(":")[1], " Insert query has mismatched column types");
        }

        hetuServer.execute("DROP TABLE testdb.unboundedchar");
    }

    @Test
    public void testFilterBoundedCharDatatype() throws SQLException
    {
        hetuServer.execute("drop table if exists testdb.boundedchar");
        hetuServer.execute("CREATE TABLE testdb.boundedchar(name char(7))");
        hetuServer.execute("INSERT INTO testdb.boundedchar VALUES('akash'),('anubav'),('bhavya'),('amit   ')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedchar ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash  ");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "amit   ");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "anubav ");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya ");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedchar WHERE NAME='akash'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash  ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedchar WHERE NAME='bhavya'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedchar WHERE NAME='amit   '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "amit   ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedchar WHERE NAME='amit'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "amit   ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("DROP TABLE testdb.boundedchar");
    }

    @Test
    public void testFilterUnboundedVarcharDatatypeForLocalDic() throws SQLException, IOException
    {
        hetuServer.execute("drop table if exists testdb.unboundedvarcharforlocaldic");
        hetuServer.execute("CREATE TABLE testdb.unboundedvarcharforlocaldic(name varchar)");
        writeSchemaFileForLocalDic("unboundedvarcharforlocaldic");
        hetuServer.execute("INSERT INTO testdb.unboundedvarcharforlocaldic VALUES('akash'),('anubhav'),('bhavya'),('amit  ')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarcharforlocaldic ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "anubhav");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarcharforlocaldic WHERE NAME='akash'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarcharforlocaldic WHERE NAME='amit  '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedvarcharforlocaldic WHERE NAME='amit'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("DROP TABLE testdb.unboundedvarcharforlocaldic");
    }

    @Test
    public void testFilterBoundedVarcharDatatypeForLocalDic() throws SQLException, IOException
    {
        hetuServer.execute("drop table if exists testdb.boundedvarcharforlocaldic");
        hetuServer.execute("CREATE TABLE testdb.boundedvarcharforlocaldic(name varchar(6))");
        writeSchemaFileForLocalDic("boundedvarcharforlocaldic");
        hetuServer.execute("INSERT INTO testdb.boundedvarcharforlocaldic VALUES('akash'),('anubav'),('bhavya'), ('amit  ')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarcharforlocaldic ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "anubav");     }});
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarcharforlocaldic WHERE NAME='akash'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "akash");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarcharforlocaldic WHERE NAME='bhavya'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "bhavya");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarcharforlocaldic WHERE NAME='amit  '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "amit  ");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarcharforlocaldic WHERE NAME='amit'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedvarcharforlocaldic WHERE NAME='amit   '");
        expectedResult = new ArrayList<Map<String, Object>>() {{
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());
        try {
            hetuServer.execute("INSERT INTO testdb.boundedvarcharforlocaldic VALUES('akashmital')");
            Assert.fail("Exception expected");
        }
        catch(SQLException e){
            assertEquals(e.getMessage().split(":")[1], " Insert query has mismatched column types");
        }

        hetuServer.execute("DROP TABLE testdb.boundedvarcharforlocaldic");
    }

    @Test
    public void testFilterUnboundedCharDatatypeForLocalDic() throws SQLException, IOException
    {
        hetuServer.execute("drop table if exists testdb.unboundedcharforlocaldic");
        hetuServer.execute("CREATE TABLE testdb.unboundedcharforlocaldic(name char)");
        writeSchemaFileForLocalDic("unboundedcharforlocaldic");
        hetuServer.execute("INSERT INTO testdb.unboundedcharforlocaldic VALUES('a'),('b'),('c'),('d')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedcharforlocaldic ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "a");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "b");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "c");      }});
            add(new TreeMap<String, Object>() {{    put("NAME", "d");      }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedcharforlocaldic WHERE NAME='a'");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "a");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());


        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.unboundedcharforlocaldic WHERE NAME='a '");
        expectedResult = new ArrayList<Map<String, Object>>()
        {{
            add(new TreeMap<String, Object>() {{    put("NAME", "a");       }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        try {
            hetuServer.execute("INSERT INTO testdb.unboundedcharforlocaldic VALUES('a ')");
            Assert.fail("Exception expected");
        }
        catch(SQLException e){
            assertEquals(e.getMessage().split(":")[1], " Insert query has mismatched column types");
        }


        hetuServer.execute("DROP TABLE testdb.unboundedcharforlocaldic");
    }

    @Test
    public void testFilterBoundedCharDatatypeForLocalDic() throws SQLException, IOException {
        hetuServer.execute("drop table if exists testdb.boundedcharlocaldic");
        hetuServer.execute("CREATE TABLE testdb.boundedcharlocaldic(id int, name char(7))");
        writeSchemaFileForLocalDic("boundedcharlocaldic");
        hetuServer.execute("INSERT INTO testdb.boundedcharlocaldic VALUES(1, 'akash'),(2, 'anubav'),(3, 'bhavya'),(4, 'amit   ')");

        List<Map<String, Object>> actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedcharlocaldic ORDER BY NAME");
        List<Map<String, Object>> expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new TreeMap<String, Object>() {{
                put("NAME", "akash  ");
            }});
            add(new TreeMap<String, Object>() {{
                put("NAME", "amit   ");
            }});
            add(new TreeMap<String, Object>() {{
                put("NAME", "anubav ");
            }});
            add(new TreeMap<String, Object>() {{
                put("NAME", "bhavya ");
            }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedcharlocaldic WHERE NAME='akash'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new TreeMap<String, Object>() {{
                put("NAME", "akash  ");
            }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedcharlocaldic WHERE NAME='bhavya'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new TreeMap<String, Object>() {{
                put("NAME", "bhavya ");
            }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedcharlocaldic WHERE NAME='amit   '");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new TreeMap<String, Object>() {{
                put("NAME", "amit   ");
            }});
        }};

        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        actualResult = hetuServer.executeQuery("SELECT NAME FROM testdb.boundedcharlocaldic WHERE NAME='amit'");
        expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new TreeMap<String, Object>() {{
                put("NAME", "amit   ");
            }});
        }};
        Assert.assertEquals(actualResult.toString(), expectedResult.toString());

        hetuServer.execute("DROP TABLE testdb.boundedcharlocaldic");
    }

    private void writeSchemaFileForLocalDic(String tableName) throws IOException {
        AbsoluteTableIdentifier identifier = AbsoluteTableIdentifier.from(storePath + "/carbon.store/testdb/" + tableName, "testdb", tableName);
        TableInfo tableInfo = SchemaReader.getTableInfo(identifier);
        tableInfo.getFactTable().getTableProperties().put("local_dictionary_enable", "true");
        String schemaFilePath = CarbonTablePath.getSchemaFilePath(storePath + "/carbon.store/testdb/" + tableName);
        SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
        ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
        thriftWriter.open(FileWriteOperation.OVERWRITE);
        thriftWriter.write(schemaConverter.fromWrapperToExternalTableInfo(tableInfo, identifier.getTableName(),
                identifier.getDatabaseName()));
        thriftWriter.close();
        FileFactory.getCarbonFile(schemaFilePath).setLastModifiedTime(System.currentTimeMillis());
        CarbonMetadata.getInstance().removeTable(identifier.getTablePath(), identifier.getDatabaseName());
        CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    }

    @Test
    public void testCheckCreateTablePartitionByNotSupported() throws SQLException
    {
        try {
            hetuServer.execute("drop table if exists testdb.partitiontesttable");
            hetuServer.execute("CREATE TABLE testdb.partitiontesttable (a int, b int , c int , d int ) WITH (partitioned_by = ARRAY['c', 'd'])");
        }
        catch (Exception e) {
            Boolean ret = e.getMessage().contains("Catalog 'carbondata' does not support table property 'partitioned_by");
            assertEquals("true", ret.toString());
            hetuServer.execute("drop table if exists testdb.partitiontesttable");
            return;
        }
        hetuServer.execute("drop table if exists testdb.partitiontesttable");
        assertEquals("true", "false");
    }

    @Test
    public void testCheckCreateTableAsPartitionByNotSupported() throws SQLException
    {
        try {
            hetuServer.execute("drop table if exists testdb.partitiontesttable1");
            hetuServer.execute("drop table if exists testdb.partitiontesttable2");
            hetuServer.execute("CREATE TABLE testdb.partitiontesttable1 (a int, b int , c int , d int )");
            hetuServer.execute("CREATE TABLE testdb.partitiontesttable2 WITH (partitioned_by = ARRAY['c', 'd']) AS SELECT *  FROM  testdb.partitiontesttable1");
        }
        catch (Exception e) {
            Boolean ret = e.getMessage().contains("Catalog 'carbondata' does not support table property 'partitioned_by");
            assertEquals("true", ret.toString());
            hetuServer.execute("drop table if exists  testdb.partitiontesttable1");
            hetuServer.execute("drop table if exists testdb.partitiontesttable2");
            return;
        }
        hetuServer.execute("drop table if exists  testdb.partitiontesttable1");
        hetuServer.execute("drop table if exists testdb.partitiontesttable2");
        assertEquals("true", "false");
    }

    @Test
    public void test_writer_count() throws SQLException
    {
        hetuServer.execute("set session task_writer_count=32");

        hetuServer.execute("CREATE TABLE testdb.testWriterCount_32(orderkey int, orderstatus STRING, totalprice double, orderdate date)");
        hetuServer.execute("INSERT INTO testdb.testWriterCount_32 VALUES(10,'SUCCESS', 125.15, DATE'2919-05-17')");
        hetuServer.execute("INSERT INTO testdb.testWriterCount_32 VALUES(20,'SUCCESS', 125.15, DATE'2919-05-17')");
        hetuServer.execute("INSERT INTO testdb.testWriterCount_32 VALUES(30,'SUCCESS', 125.15, DATE'2919-05-17')");

        hetuServer.execute("INSERT INTO testdb.testWriterCount_32 SELECT * FROM testdb.testWriterCount_32");
        verifyRowCount("testdb.testWriterCount_32", 6);

        hetuServer.execute("INSERT INTO testdb.testWriterCount_32 SELECT * FROM testdb.testWriterCount_32");
        verifyRowCount("testdb.testWriterCount_32", 12);

        hetuServer.execute("INSERT INTO testdb.testWriterCount_32 SELECT * FROM testdb.testWriterCount_32");
        verifyRowCount("testdb.testWriterCount_32", 24);

        String filePath = storePath + "/carbon.store/testdb/testwritercount_32";
        try {
            hetuServer.execute("VACUUM TABLE testdb.testWriterCount_32 AND WAIT");
            assertTrue(FileFactory.isFileExist(filePath + "/Fact/Part0/Segment_0.1"));
        }
        catch (IOException e) {
            assertTrue(false, "Unable to read file from table path");
        }
        finally {
            hetuServer.execute("drop table if exists testdb.testWriterCount_32");
            hetuServer.execute("set session task_writer_count=1");
        }
    }

    private void verifyRowCount(String tableName, int rowCount) throws SQLException
    {
        String query = String.format("SELECT COUNT(*) AS result FROM %s", tableName);
        List<Map<String, Object>>  actualResult = hetuServer.executeQuery(query);
        List<Map<String, Object>>  expectedResult = new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{    put("result", rowCount); }});
        }};
        Assert.assertEquals(actualResult.toString(), expectedResult.toString());
    }

    private TableInfo getTableInfoFromSchemaFile(String tableName)
    {
        AbsoluteTableIdentifier identifier = AbsoluteTableIdentifier.from(storePath + "/carbon.store/testdb/" + tableName, "testdb", tableName);
        TableInfo tableInfo = null;
        try {
            tableInfo = SchemaReader.getTableInfo(identifier);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to read schema file", e);
        }
        return tableInfo;
    }
}
