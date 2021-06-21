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
package io.prestosql.plugin.splitmanager;

import io.airlift.concurrent.SetThreadName;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.plugin.splitmanager.TableSplitUtil.generateTableFullName;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StepCalcManager
        implements Runnable
{
    private static final Logger log = Logger.get(StepCalcManager.class);
    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private PriorityBlockingQueue<TableSplitConfig> adjustTableQueue;
    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();
    private volatile boolean closed;
    private final int stepCalcThreads;
    private Duration refreshInterval;
    private long lastUpdateTime;
    private ConcurrentHashMap<String, TableSplitConfig> tableList;
    private ConcurrentHashMap<String, List<SplitStatLog>> splitRunLog;
    private ConcurrentHashMap<String, List<SplitStatLog>> splitAdjustLog;

    public StepCalcManager(Duration pdboRefreshInverval,
            int stepCalcThreads,
            List<TableSplitConfig> tableList)
    {
        this.executor = newCachedThreadPool(threadsNamed("step-calc-processor-%d"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);

        requireNonNull(tableList, "Table split config list is null.");
        this.tableList = new ConcurrentHashMap();
        for (TableSplitConfig table : tableList) {
            this.tableList.put(generateTableFullName(table.getCatalogName(), table.getSchemaName(), table.getTableName()), table);
        }
        this.adjustTableQueue = new PriorityBlockingQueue<>((tableList.size() > 0) ? tableList.size() * 2 : 10);
        this.closed = false;
        this.lastUpdateTime = 0L;
        this.refreshInterval = pdboRefreshInverval;
        this.stepCalcThreads = stepCalcThreads;

        this.splitRunLog = new ConcurrentHashMap();
        this.splitAdjustLog = new ConcurrentHashMap();
    }

    @Override
    public void run()
    {
        while (!closed) {
            try {
                if (lastUpdateTime == 0L) {
                    addTableInfo();
                }
                else {
                    Thread.sleep(refreshInterval.toMillis());
                    addTableInfo();
                }
                lastUpdateTime = System.currentTimeMillis();
            }
            catch (Exception e) {
                log.error("Load pdbo table error : ", e.getMessage());
                lastUpdateTime = System.currentTimeMillis();
            }
        }
    }

    public void addTableInfo()
    {
        if (adjustTableQueue.isEmpty()) {
            List<TableSplitConfig> configList = new ArrayList<>();
            for (Map.Entry<String, TableSplitConfig> entry : this.tableList.entrySet()) {
                configList.add(entry.getValue());
            }
            startSplit(configList);
        }
    }

    public void updateTableConfig(List<TableSplitConfig> configList)
    {
        requireNonNull(configList, "Table split config list is null.");
        this.tableList = new ConcurrentHashMap();
        for (TableSplitConfig table : configList) {
            this.tableList.put(generateTableFullName(table.getCatalogName(), table.getSchemaName(), table.getTableName()), table);
        }
    }

    private static List<SplitStatLog> copySplitStatLogList(List<SplitStatLog> list)
    {
        if ((list == null) || (list.isEmpty())) {
            return null;
        }

        List<SplitStatLog> copyLogList = new ArrayList<>();
        for (SplitStatLog statLog : list) {
            SplitStatLog log = new SplitStatLog();
            log.setCatalogName(statLog.getCatalogName())
                    .setSchemaName(statLog.getSchemaName())
                    .setTableName(statLog.getTableName())
                    .setSplitField(statLog.getSplitField())
                    .setSplitCount(statLog.getSplitCount())
                    .setRows(statLog.getRows())
                    .setBeginIndex(statLog.getBeginIndex())
                    .setEndIndex(statLog.getEndIndex())
                    .setTimeStamp(statLog.getTimeStamp())
                    .setRecordFlag(statLog.getRecordFlag());
            copyLogList.add(log);
        }

        return copyLogList;
    }

    // commit split run stat log
    public void commitSplitStatLog(List<SplitStatLog> list)
    {
        if ((list == null) || (list.isEmpty())) {
            return;
        }

        List<SplitStatLog> runLogList = copySplitStatLogList(list);

        // sort tableLogs by beginIndex
        Collections.sort(runLogList, new Comparator<SplitStatLog>()
        {
            @Override
            public int compare(SplitStatLog splitStatLog, SplitStatLog t1)
            {
                return (splitStatLog.getBeginIndex().longValue() > t1.getBeginIndex().longValue()) ? 1 : -1;
            }
        });

        SplitStatLog firstLog = runLogList.get(0);
        TableSplitConfig table = this.tableList.get(generateTableFullName(firstLog.getCatalogName(),
                firstLog.getSchemaName(), firstLog.getTableName()));
        if ((table == null) || (!checkSplitLog(table, runLogList))) {
            return;
        }

        String key = generateTableFullName(firstLog.getCatalogName(),
                firstLog.getSchemaName(),
                firstLog.getTableName());

        splitRunLog.remove(key);
        splitRunLog.put(key, runLogList);

        return;
    }

    private synchronized void startSplit(List<TableSplitConfig> tables)
    {
        adjustTableQueue.addAll(tables);
    }

    @PostConstruct
    public synchronized void start()
    {
        checkState(!closed, "StepCalcManager is closed");
        for (int i = 0; i < stepCalcThreads; i++) {
            addRunnerThread(new AdjustRunner());
        }
    }

    @PreDestroy
    public synchronized void stop()
    {
        closed = true;
        executor.shutdownNow();
    }

    private synchronized void addRunnerThread(Runnable runner)
    {
        try {
            executor.execute(runner);
        }
        catch (RejectedExecutionException ignored) {
        }
    }

    // TableSplitConfig.catalogName/schemaName/tableName
    public List<SplitStatLog> getAdjustSplitList(String catalogName, String schemaName, String tableName)
    {
        String key = generateTableFullName(catalogName, schemaName, tableName);
        List<SplitStatLog> result = splitAdjustLog.get(key);
        List<SplitStatLog> resultLogList = null;
        SplitStatLog resultLog;
        if (result != null) {
            resultLogList = copySplitStatLogList(result);
        }

        return resultLogList;
    }

    public static List<SplitStatLog> stepCalcProc(TableSplitConfig table, List<SplitStatLog> tableLogList)
    {
        Boolean successed = false;

        List<SplitStatLog> tableLogs = copySplitStatLogList(tableLogList);

        // sort tableLogs by beginIndex
        Collections.sort(tableLogs, new Comparator<SplitStatLog>()
        {
            @Override
            public int compare(SplitStatLog splitStatLog, SplitStatLog t1)
            {
                return (splitStatLog.getBeginIndex().longValue() > t1.getBeginIndex().longValue()) ? 1 : -1;
            }
        });

        if (!checkSplitLog(table, tableLogs)) {
            return null;
        }

        List<SplitStatLog> result = new ArrayList<>();
        successed = adjustTableSplit(tableLogs, result);

        if (successed) {
            SplitStatLog firstLog = tableLogs.get(0);
            for (SplitStatLog statLog : result) {
                statLog.setSplitField(firstLog.getSplitField())
                        .setRecordFlag(SplitStatLog.LogState.STATE_FINISH);
            }
        }
        else {
            result.clear();
            return null;
        }

        return result;
    }

    private static Boolean checkSplitLog(TableSplitConfig table, List<SplitStatLog> tableLogs)
    {
        int totalSplitNum = tableLogs.size();
        SplitStatLog firstLog = tableLogs.get(0);

        if (firstLog.getSplitCount() == null) {
            log.debug("scan node is null, split run log invalid. table=" + table.getTableName());
            return false;
        }

        if (firstLog.getSplitCount() != totalSplitNum) {
            // scanNodes + upper limit + lower limit
            if ((firstLog.getSplitCount() + 2) != totalSplitNum) {
                log.debug("split run log invalid. logCount=" + totalSplitNum +
                        ",expect=" + firstLog.getSplitCount() + ", table=" + table.getTableName());
                return false;
            }
        }

        if (!table.getSplitField().equals(firstLog.getSplitField())) {
            log.debug("split run log invalid. config field=" + table.getSplitField() +
                    ",field in log=" + firstLog.getSplitField() + ", table=" + table.getTableName());
            return false;
        }

        if (!table.getSplitCount().equals(firstLog.getSplitCount())) {
            log.debug("split run log invalid. config scanNode=" + table.getSplitCount() +
                    ",log splitCount=" + firstLog.getSplitCount() + ", table=" + table.getTableName());
            return false;
        }

        if (System.nanoTime() < firstLog.getTimeStamp()) {
            return false;
        }

        long rangeEnd;
        rangeEnd = tableLogs.get(0).getBeginIndex();
        for (SplitStatLog statLog : tableLogs) {
            // split range of logs must be continuous
            if (rangeEnd != statLog.getBeginIndex()) {
                log.debug("split run log invalid for range not continous. last end=" + rangeEnd +
                        ",range begin=" + statLog.getBeginIndex() + ", table=" + table.getTableName());
                return false;
            }

            if (statLog.getBeginIndex() >= statLog.getEndIndex()) {
                log.debug("split run log invalid for range invalid. range begin=" + statLog.getBeginIndex() +
                        ",range end=" + statLog.getEndIndex() + ", table=" + table.getTableName());
                return false;
            }

            if (statLog.getRows() < 0) {
                return false;
            }

            rangeEnd = statLog.getEndIndex();
        }

        return true;
    }

    private static Boolean adjustTableSplit(List<SplitStatLog> tableLogs, List<SplitStatLog> result)
    {
        Boolean success;
        List<SplitStatLog> tmp = new ArrayList<>();
        long logCount = tableLogs.size();
        int logIndex = 0;
        long sum = 0;
        long allRows = tableLogs.stream().mapToLong(SplitStatLog::getRows).sum();
        long averageRows = allRows / tableLogs.size();

        if (averageRows <= 0) {
            return false;
        }

        /* note: value[0] stand for filed min value and value[1] stand for field max value
                 for split devide use, not actual min/max field value
        */
        Long[] fieldRangeValue;
        fieldRangeValue = getSplitLogFieldRange(tableLogs);

        for (SplitStatLog split : tableLogs) {
            logIndex++;

            if (averageRows > (sum + split.getRows())) {
                tmp.add(split);
                sum += split.getRows();
                if (logIndex == logCount) {
                    if (result.size() == logCount) {
                        result.get(result.size() - 1).setEndIndex(split.getEndIndex());
                    }
                    else {
                        combineResult(tmp, result);
                    }
                    tmp.clear();
                    sum = 0;
                }
            }
            else {
                success = generateSplit(allRows, fieldRangeValue, averageRows, tmp, split, result);
                if (!success) {
                    return false;
                }

                sum = tmp.stream().mapToLong(SplitStatLog::getRows).sum();
            }
        }

        if (!tmp.isEmpty()) {
            if (result.size() == tableLogs.size()) {
                result.get(result.size() - 1).setEndIndex(tableLogs.get(tableLogs.size() - 1).getEndIndex());
            }
            else {
                combineResult(tmp, result);
            }
        }

        if (result.size() != logCount) {
            log.debug("adjust step failed, origin logsize=" + logCount + ",result logsize=" + result.size() + ",average rows=" + averageRows);
            return false;
        }

        return true;
    }

    private static Long[] getSplitLogFieldRange(List<SplitStatLog> tableLogs)
    {
        Long[] rangeValue = new Long[2];
        SplitStatLog firstLog = tableLogs.get(0);
        SplitStatLog lastLog = tableLogs.get(tableLogs.size() - 1);

        if (firstLog.getBeginIndex() == Long.MIN_VALUE) {
            // begin index is unbound, use end index instead
            rangeValue[0] = firstLog.getEndIndex();
        }
        else {
            rangeValue[0] = firstLog.getBeginIndex();
        }

        if (lastLog.getEndIndex() == Long.MAX_VALUE) {
            // end index is unbound, use begin index instead
            rangeValue[1] = lastLog.getBeginIndex();
        }
        else {
            rangeValue[1] = lastLog.getEndIndex();
        }

        return rangeValue;
    }

    private static void combineResult(List<SplitStatLog> tmp, List<SplitStatLog> result)
    {
        result.add(new SplitStatLog()
                .setCatalogName(tmp.get(0).getCatalogName())
                .setSchemaName(tmp.get(0).getSchemaName())
                .setTableName(tmp.get(0).getTableName())
                .setRows(tmp.stream().mapToLong(SplitStatLog::getRows).sum())
                .setSplitCount(tmp.get(0).getSplitCount())
                .setBeginIndex(tmp.get(0).getBeginIndex())
                .setEndIndex(tmp.get(tmp.size() - 1).getEndIndex()));
    }

    private static Boolean generateSplit(long allRows, Long[] fieldRangeValue, long averageRows, List<SplitStatLog> gatherList,
            SplitStatLog split, List<SplitStatLog> result)
    {
        long sum = 0;
        long moreRows = 0;
        long cutSteps;
        long beginIndex;
        long endIndex;
        long splitOffset = 0;
        long leftRows;

        long nowSum = gatherList.stream().mapToLong(SplitStatLog::getRows).sum();

        // the way of getting [beginIndex, endIndex) for a sub split special to normal way
        if ((split.getBeginIndex() == Long.MIN_VALUE) && split.getRows() >= averageRows) {
            splitLowerBoundSplit(allRows, fieldRangeValue, averageRows, gatherList, split, result);
            return true;
        }

        SplitStatLog subSplit;
        leftRows = split.getRows();
        splitOffset = split.getBeginIndex();
        if (!gatherList.isEmpty()) {
            moreRows = averageRows - nowSum;
            if (split.getEndIndex() == Long.MAX_VALUE) {
                cutSteps = moreRows * (fieldRangeValue[1] - fieldRangeValue[0]) / allRows;
            }
            else {
                cutSteps = moreRows * (split.getEndIndex() - split.getBeginIndex()) / split.getRows();
            }
            cutSteps = (cutSteps > 0) ? cutSteps : 1; // make sure beginIndex  != endIndex
            beginIndex = split.getBeginIndex();
            endIndex = (moreRows == split.getRows()) ? split.getEndIndex() : split.getBeginIndex() + cutSteps;

            subSplit = new SplitStatLog();
            subSplit.setCatalogName(split.getCatalogName())
                    .setSchemaName(split.getSchemaName())
                    .setTableName(split.getTableName())
                    .setRows(moreRows)
                    .setSplitCount(split.getSplitCount())
                    .setBeginIndex(beginIndex)
                    .setEndIndex(endIndex)
                    .setRecordFlag(SplitStatLog.LogState.STATE_FINISH);
            gatherList.add(subSplit);
            combineResult(gatherList, result);
            gatherList.clear();

            splitOffset = endIndex;
            leftRows = split.getRows() - moreRows;
        }

        while (leftRows >= averageRows) {
            subSplit = new SplitStatLog();
            if (split.getEndIndex() == Long.MAX_VALUE) {
                cutSteps = averageRows * (fieldRangeValue[1] - fieldRangeValue[0]) / allRows;
            }
            else {
                cutSteps = averageRows * (split.getEndIndex() - split.getBeginIndex()) / split.getRows();
            }
            cutSteps = (cutSteps > 0) ? cutSteps : 1; // make sure beginIndex  != endIndex
            beginIndex = splitOffset;
            endIndex = (leftRows == averageRows) ? split.getEndIndex() : splitOffset + cutSteps;
            if (endIndex > split.getEndIndex()) {
                return false;
            }

            subSplit.setCatalogName(split.getCatalogName())
                    .setSchemaName(split.getSchemaName())
                    .setTableName(split.getTableName())
                    .setRows(averageRows)
                    .setSplitCount(split.getSplitCount())
                    .setBeginIndex(beginIndex)
                    .setEndIndex(endIndex)
                    .setRecordFlag(SplitStatLog.LogState.STATE_FINISH);
            result.add(subSplit);

            splitOffset += cutSteps;
            leftRows -= averageRows;
        }

        if (leftRows > 0) {
            split.setRows(leftRows)
                    .setBeginIndex(splitOffset);
            if (split.getBeginIndex() >= split.getEndIndex()) {
                return false;
            }
            gatherList.add(split);
        }

        return true;
    }

    private static void splitLowerBoundSplit(long allRows, Long[] fieldRangeValue, long averageRows, List<SplitStatLog> gatherList,
            SplitStatLog split, List<SplitStatLog> result)
    {
        long beginIndex;
        long endIndex;
        long splitOffset = 0;
        long cutSteps;
        long modRows;
        long leftRows;

        SplitStatLog subSplit;
        leftRows = split.getRows();
        splitOffset = split.getEndIndex();

        modRows = split.getRows() % averageRows;
        if (modRows != 0) {
            cutSteps = modRows * (fieldRangeValue[1] - fieldRangeValue[0]) / allRows;
            cutSteps = (cutSteps > 0) ? cutSteps : 1;
            beginIndex = splitOffset - cutSteps;
            endIndex = split.getEndIndex();

            subSplit = new SplitStatLog();
            subSplit.setCatalogName(split.getCatalogName())
                    .setSchemaName(split.getSchemaName())
                    .setTableName(split.getTableName())
                    .setRows(modRows)
                    .setSplitCount(split.getSplitCount())
                    .setBeginIndex(beginIndex)
                    .setEndIndex(endIndex)
                    .setRecordFlag(SplitStatLog.LogState.STATE_FINISH);

            gatherList.clear();
            gatherList.add(subSplit); // will merge with later splits

            splitOffset = beginIndex;
            leftRows = split.getRows() - modRows;
        }

        while (leftRows >= averageRows) {
            subSplit = new SplitStatLog();
            cutSteps = averageRows * (fieldRangeValue[1] - fieldRangeValue[0]) / allRows;
            cutSteps = (cutSteps > 0) ? cutSteps : 1; // make sure beginIndex  != endIndex
            if (averageRows == leftRows) {
                // the split which contain field lower boundary
                beginIndex = split.getBeginIndex();
            }
            else {
                beginIndex = splitOffset - cutSteps;
            }
            endIndex = splitOffset;
            subSplit.setCatalogName(split.getCatalogName())
                    .setSchemaName(split.getSchemaName())
                    .setTableName(split.getTableName())
                    .setRows(averageRows)
                    .setSplitCount(split.getSplitCount())
                    .setBeginIndex(beginIndex)
                    .setEndIndex(endIndex)
                    .setRecordFlag(SplitStatLog.LogState.STATE_FINISH);
            result.add(subSplit);

            splitOffset = beginIndex;
            leftRows -= averageRows;
        }

        return;
    }

    private class AdjustRunner
            implements Runnable
    {
        private final long runnerId = NEXT_RUNNER_ID.getAndIncrement();

        @Override
        public void run()
        {
            try (SetThreadName runnerName = new SetThreadName("SplitRunner-%s", runnerId)) {
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    final TableSplitConfig tableSplitConfig;
                    try {
                        tableSplitConfig = adjustTableQueue.take();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    stepCalc(tableSplitConfig);
                }
            }
            finally {
                if (!closed) {
                    addRunnerThread(new AdjustRunner());
                }
            }
        }

        public void stepCalc(TableSplitConfig table)
        {
            List<SplitStatLog> calcStepLogList;

            if (!table.isCalcStepEnable()) {
                // clean step calc log if exist
                String key = generateTableFullName(table.getCatalogName(),
                        table.getSchemaName(),
                        table.getTableName());
                splitAdjustLog.remove(key);

                return;
            }

            List<SplitStatLog> tableLogs = getTableRunningLogs(table);
            if (tableLogs == null || tableLogs.size() == 0) {
                return;
            }

            calcStepLogList = getAdjustSplitList(table.getCatalogName(), table.getSchemaName(), table.getTableName());
            if ((calcStepLogList != null) && (!calcStepLogList.isEmpty())) {
                // run logs have been calculated, expect newer run logs
                if (calcStepLogList.get(0).getTimeStamp() > tableLogs.get(0).getTimeStamp()) {
                    return;
                }
            }

            List<SplitStatLog> result = stepCalcProc(table, tableLogs);

            if ((result != null) && (!result.isEmpty())) {
                saveAdjustLogs(result);
            }
        }

        public List<SplitStatLog> getTableRunningLogs(TableSplitConfig tableConfig)
        {
            String key = generateTableFullName(tableConfig.getCatalogName(),
                    tableConfig.getSchemaName(),
                    tableConfig.getTableName());

            return splitRunLog.get(key);
        }

        public void saveAdjustLogs(List<SplitStatLog> result)
        {
            long timestamp = System.nanoTime();
            for (SplitStatLog split : result) {
                split.setTimeStamp(timestamp);
            }

            SplitStatLog firstLog = result.get(0);
            String key = generateTableFullName(firstLog.getCatalogName(),
                    firstLog.getSchemaName(),
                    firstLog.getTableName());

            splitAdjustLog.remove(key);
            splitAdjustLog.put(key, result);
        }
    }
}
