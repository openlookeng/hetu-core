
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
package io.prestosql.plugin.splitmanager;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStepCalcManage
{
    @Test
    // all splits are equally distributed, no adjust
    public void testDynamicStepCalc0()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 20},
                        {0, 35, 20},
                        {35, 70, 20},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == rangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == rangeArray[i][1]);
            assertTrue(splitLog.getRows() == rangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // [Long.MIN_VALUE, some_A) has a rowCount of 0
    public void testDynamicStepCalc1()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 0},
                        {0, 35, 40},
                        {35, 70, 20},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };
        long[][] resRangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 17, 20},
                        {17, 35, 20},
                        {35, 70, 20},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // [Long.MIN_VALUE, some_A) has a rowCount of 0, and split[1] needs split and devide into several splits
    public void testDynamicStepCalc2()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 0},
                        {0, 35, 45},
                        {35, 70, 15},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };
        long[][] resRangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 15, 20},
                        {15, 30, 20},
                        {30, 70, 20},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // splits merge into one
    public void testDynamicStepCalc3()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 0},
                        {0, 35, 10},
                        {35, 70, 10},
                        {70, 100, 60},
                        {100, Long.MAX_VALUE, 20}
                };
        long[][] resRangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 70, 20},
                        {70, 80, 20},
                        {80, 90, 20},
                        {90, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // [some_B, Long.MAX_VALUE) has a rowCount of 0
    public void testDynamicStepCalc4()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 20},
                        {0, 35, 20},
                        {35, 70, 20},
                        {70, 100, 40},
                        {100, Long.MAX_VALUE, 0}
                };
        long[][] resRangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 20},
                        {0, 35, 20},
                        {35, 70, 20},
                        {70, 85, 20},
                        {85, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // [Long.MIN_VALUE, some_A) needs devide into several splits
    public void testDynamicStepCalc5()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 30},
                        {0, 35, 10},
                        {35, 70, 20},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };
        long[][] resRangeArray = new long[][]
                {
                        {Long.MIN_VALUE, -10, 20},
                        {-10, 35, 20},
                        {35, 70, 20},
                        {70, 100, 20},
                        {100, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // [some_A, Long.MAX_VALUE) needs devide into several splits
    public void testDynamicStepCalc6()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 3;
        long timeStamp = System.nanoTime();
        long rangeBound;
        // scanNodes+2 splits, includes [Long.MIN_VALUE, some_A) [some_B, Long.MAX_VALUE)
        long[][] rangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 20},
                        {0, 35, 20},
                        {35, 70, 20},
                        {70, 100, 10},
                        {100, Long.MAX_VALUE, 30}
                };
        long[][] resRangeArray = new long[][]
                {
                        {Long.MIN_VALUE, 0, 20},
                        {0, 35, 20},
                        {35, 70, 20},
                        {70, 110, 20},
                        {110, Long.MAX_VALUE, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < (scanNodes + 2); i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            i++;
        }

        return;
    }

    @Test
    // fixed bound split adjust
    public void testDynamicStepCalc7()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        List<SplitStatLog> tableLogs;
        List<SplitStatLog> result;
        int scanNodes = 5;
        long rangeBound;
        long timeStamp = System.nanoTime();
        long[][] rangeArray = new long[][]
                {
                        {0, 20, 15},
                        {20, 40, 20},
                        {40, 60, 45},
                        {60, 80, 10},
                        {80, 100, 10}
                };
        long[][] resRangeArray = new long[][]
                {
                        {0, 25, 20},
                        {25, 42, 20},
                        {42, 50, 20},
                        {50, 60, 20},
                        {60, 100, 20}
                };

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < scanNodes; i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        result = StepCalcManager.stepCalcProc(tableConfig, tableLogs);
        assertEquals(result.size(), tableLogs.size());
        assertTrue(result.get(0).getBeginIndex() == tableLogs.get(0).getBeginIndex());
        assertTrue(result.get(result.size() - 1).getEndIndex() == tableLogs.get(tableLogs.size() - 1).getEndIndex());
        int i = 0;
        rangeBound = rangeArray[0][0];
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // object interface test, fixed bound split adjust
    public void testDynamicStepCalc8()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;
        long rangeBound;
        long[][] resRangeArray = new long[][]
                {
                        {0, 25, 20},
                        {25, 42, 20},
                        {42, 50, 20},
                        {50, 60, 20},
                        {60, 100, 20}
                };

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result.size(), tableLogs.size());
        assertTrue(result.get(0).getBeginIndex() == tableLogs.get(0).getBeginIndex());
        assertTrue(result.get(result.size() - 1).getEndIndex() == tableLogs.get(tableLogs.size() - 1).getEndIndex());
        int i = 0;
        rangeBound = tableLogs.get(0).getBeginIndex();
        for (SplitStatLog splitLog : result) {
            assertTrue(rangeBound == splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertTrue(splitLog.getBeginIndex() == resRangeArray[i][0]);
            assertTrue(splitLog.getEndIndex() == resRangeArray[i][1]);
            assertTrue(splitLog.getRows() == resRangeArray[i][2]);
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        return;
    }

    @Test
    // object interface test: no adjust log for tables that disable step calc
    public void testDynamicStepCalc9()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        tableConfig.setCalcStepEnable(false);
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result, null);

        return;
    }

    @Test
    // object interface test: no adjust log for tables that log count error
    public void testDynamicStepCalc10()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        tableLogs.remove(1); // remove log to simulate log count error
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result, null);

        return;
    }

    @Test
    // object interface test: no adjust log for tables that log range error
    public void testDynamicStepCalc11()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        tableLogs.get(1).setBeginIndex(tableLogs.get(0).getEndIndex() + 10); // set error to breaklog range continous
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result, null);

        return;
    }

    @Test
    // object interface test: no new run log test, ensure run log calc not more than one time
    public void testDynamicStepCalc12()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;
        long timeStamp;

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result.size(), tableLogs.size());

        timeStamp = result.get(0).getTimeStamp();
        for (SplitStatLog runLog : tableLogs) {
            runLog.setTimeStamp(timeStamp - 100); // set to older time
        }
        stepCalcManager.commitSplitStatLog(tableLogs);

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result.size(), tableLogs.size());

        // calc log not update
        assertTrue(timeStamp == result.get(0).getTimeStamp());

        return;
    }

    @Test
    // object interface test, rows too few to ajust
    public void testDynamicStepCalc13()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;

        // set rows to a very small value, average row count is 1
        tableLogs.get(0).setRows(5L);
        tableLogs.get(1).setRows(0L);
        tableLogs.get(2).setRows(2L);
        tableLogs.get(3).setRows(1L);
        tableLogs.get(4).setRows(0L);

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertTrue(result == null);

        return;
    }

    @Test
    // object interface test: dynamic rows change, use old adjust result if unable tp calc new step
    public void testDynamicStepCalc14()
    {
        TableSplitConfig tableConfig = getDefaultConfig();
        List<SplitStatLog> tableLogs = getDefaultTableLogs(tableConfig);
        List<SplitStatLog> result;
        long rangeBound;
        long[][] resRangeArray = new long[][]
                {
                        {0, 25, 20},
                        {25, 42, 20},
                        {42, 50, 20},
                        {50, 60, 20},
                        {60, 100, 20}
                };

        StepCalcManager stepCalcManager;
        stepCalcManager = new StepCalcManager(new Duration(2, TimeUnit.MILLISECONDS),
                1, ImmutableList.of(tableConfig));
        stepCalcManager.commitSplitStatLog(tableLogs);
        Thread stepCalcThread = new Thread(stepCalcManager);
        stepCalcThread.setName("step calc thread");
        stepCalcThread.setDaemon(true);
        stepCalcThread.start();
        stepCalcManager.start();

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }
        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result.size(), tableLogs.size());
        assertEquals(result.get(0).getBeginIndex(), tableLogs.get(0).getBeginIndex());
        assertEquals(result.get(result.size() - 1).getEndIndex(), tableLogs.get(tableLogs.size() - 1).getEndIndex());
        int i = 0;
        rangeBound = tableLogs.get(0).getBeginIndex();
        for (SplitStatLog splitLog : result) {
            assertEquals(Long.valueOf(rangeBound), splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertEquals(splitLog.getBeginIndex(), Long.valueOf(resRangeArray[i][0]));
            assertEquals(splitLog.getEndIndex(), Long.valueOf(resRangeArray[i][1]));
            assertEquals(splitLog.getRows(), Long.valueOf(resRangeArray[i][2]));
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }

        // simulate record deleted, average split row count is less than 1
        long timeStamp = System.nanoTime();
        tableLogs.get(0).setTimeStamp(timeStamp).setRows(2L);
        tableLogs.get(1).setTimeStamp(timeStamp).setRows(0L);
        tableLogs.get(2).setTimeStamp(timeStamp).setRows(0L);
        tableLogs.get(3).setTimeStamp(timeStamp).setRows(1L);
        tableLogs.get(4).setTimeStamp(timeStamp).setRows(1L);
        stepCalcManager.commitSplitStatLog(tableLogs);

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            e.getMessage();
        }

        result = stepCalcManager.getAdjustSplitList(tableConfig.getCatalogName(), tableConfig.getSchemaName(), tableConfig.getTableName());
        assertEquals(result.size(), tableLogs.size());
        assertEquals(result.get(0).getBeginIndex(), tableLogs.get(0).getBeginIndex());
        assertEquals(result.get(result.size() - 1).getEndIndex(), tableLogs.get(tableLogs.size() - 1).getEndIndex());

        // new run log rows too few, can not generate new split, stay with old splits
        i = 0;
        rangeBound = tableLogs.get(0).getBeginIndex();
        for (SplitStatLog splitLog : result) {
            assertEquals(Long.valueOf(rangeBound), splitLog.getBeginIndex());
            rangeBound = splitLog.getEndIndex();
            assertEquals(splitLog.getBeginIndex(), Long.valueOf(resRangeArray[i][0]));
            assertEquals(splitLog.getEndIndex(), Long.valueOf(resRangeArray[i][1]));
            assertEquals(splitLog.getRows(), Long.valueOf(resRangeArray[i][2]));
            assertEquals(splitLog.getSplitField(), tableConfig.getSplitField());
            assertEquals(splitLog.getScanNodes(), tableConfig.getScanNodes());
            assertEquals(splitLog.getRecordFlag(), SplitStatLog.LogState.STATE_FINISH);
            i++;
        }
    }

    private void setLogRange(long[][] rangeArray, List<SplitStatLog> tableLogs)
    {
        int i = 0;

        for (SplitStatLog splitLog : tableLogs) {
            splitLog.setBeginIndex(rangeArray[i][0])
                    .setEndIndex(rangeArray[i][1])
                    .setRows(rangeArray[i][2]);
            i++;
        }
    }

    private TableSplitConfig getDefaultConfig()
    {
        TableSplitConfig tableConfig = new TableSplitConfig();
        int scanNodes = 5;

        tableConfig.setCatalogName("test_catalog");
        tableConfig.setSchemaName("test_schema");
        tableConfig.setTableName("test_table");
        tableConfig.setCalcStepEnable(true);
        tableConfig.setSplitField("test_field");
        tableConfig.setScanNodes(scanNodes);

        return tableConfig;
    }

    private List<SplitStatLog> getDefaultTableLogs(TableSplitConfig tableConfig)
    {
        List<SplitStatLog> tableLogs;
        int scanNodes = 5;
        long timeStamp = System.nanoTime();
        long[][] rangeArray = new long[][]
                {
                        {0, 20, 15},
                        {20, 40, 20},
                        {40, 60, 45},
                        {60, 80, 10},
                        {80, 100, 10}
                };

        tableLogs = new ArrayList<SplitStatLog>();
        for (int i = 0; i < scanNodes; i++) {
            SplitStatLog tableLog = new SplitStatLog();
            tableLog.setCatalogName(tableConfig.getCatalogName())
                    .setSchemaName(tableConfig.getSchemaName())
                    .setTableName(tableConfig.getTableName())
                    .setScanNodes(tableConfig.getScanNodes())
                    .setSplitField(tableConfig.getSplitField())
                    .setTimeStamp(timeStamp)
                    .setRecordFlag(SplitStatLog.LogState.STATE_NEW);
            tableLogs.add(tableLog);
        }

        setLogRange(rangeArray, tableLogs);

        return tableLogs;
    }
}
