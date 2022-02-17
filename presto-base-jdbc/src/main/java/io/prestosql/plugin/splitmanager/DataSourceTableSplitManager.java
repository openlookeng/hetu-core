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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.splitmanager.TableSplitUtil.generateTableFullName;

public class DataSourceTableSplitManager
{
    private static final Logger log = Logger.get(DataSourceTableSplitManager.class);

    private boolean enableTableSplit;

    private Map<String, TableSplitConfig> tableSplitsMap = new HashMap<>();

    private StepCalcManager stepCalcManager;

    private JdbcClient jdbcClient;

    @Inject
    public DataSourceTableSplitManager(BaseJdbcConfig config, JdbcClient jdbcClient, NodeManager nodeManager)
    {
        this.enableTableSplit = config.getTableSplitEnable();
        this.jdbcClient = jdbcClient;
        if (enableTableSplit && nodeManager.getCurrentNode().isCoordinator()) {
            List<TableSplitConfig> splitConfigs = loadTableSplitFiledConfig(config.getTableSplitFields());
            tableSplitsMap = splitConfigs.stream().collect(Collectors.toMap(
                    p -> generateTableFullName(p.getCatalogName(), p.getSchemaName(), p.getTableName()),
                    Function.identity()));
            stepCalcManager = new StepCalcManager(config.getTableSplitStepCalcRefreshInterval(),
                                                config.getTableSplitStepCalcCalcThreads(), splitConfigs);
            Thread stepCalcThread = new Thread(stepCalcManager);
            stepCalcThread.setName("step calc thread");
            stepCalcThread.setUncaughtExceptionHandler((tr, ex) -> System.out.println(tr.getName() + " : " + ex.getMessage()));
            stepCalcThread.setDaemon(true);
            stepCalcThread.start();
            stepCalcManager.start();
        }
    }

    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        if (enableTableSplit) {
            return getTableSplits(identity, tableHandle);
        }

        return getFixedSplitSource(tableHandle);
    }

    private List<TableSplitConfig> loadTableSplitFiledConfig(String jsonConfigContext)
    {
        if (isNullOrEmpty(jsonConfigContext)) {
            return new ArrayList<TableSplitConfig>();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(jsonConfigContext, new TypeReference<List<TableSplitConfig>>() { });
        }
        catch (JsonProcessingException e) {
            log.error("loading table split field, error info:" + e.getMessage());
        }
        return new ArrayList<TableSplitConfig>();
    }

    private void handleSplitStatic(JdbcIdentity identity, List<JdbcSplit> jdbcSplitList)
    {
        List<SplitStatLog> splitStatLogList = jdbcClient.getSplitStatic(identity, jdbcSplitList);
        stepCalcManager.commitSplitStatLog(splitStatLogList);
    }

    public TableSplitConfig getTableSplitConfig(JdbcTableHandle jdbcTableHandle)
    {
        if ((null == jdbcTableHandle) || !enableTableSplit) {
            return null;
        }
        return tableSplitsMap.get(generateTableFullName(jdbcTableHandle.getCatalogName(), jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName()));
    }

    public ConnectorSplitSource getTableSplits(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        List<JdbcSplit> jdbcSplitsList = new ArrayList<>();
        TableSplitConfig splitConfig = tableSplitsMap.get(generateTableFullName(jdbcTableHandle.getCatalogName(), jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName()));
        if (splitConfig == null) {
            return getFixedSplitSource(jdbcTableHandle);
        }
        if (splitConfig.getSplitCount() == null || isNullOrEmpty(splitConfig.getSplitField()) || splitConfig.getSplitCount() <= 1) {
            return getFixedSplitSource(jdbcTableHandle);
        }
        //config with wrong split field type
        if (!splitConfig.isTableSplitFieldValid()) {
            log.warn("Table(%s) split field(%s) NOT integer like value type.", generateTableFullName(jdbcTableHandle.getCatalogName(), jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName()),
                    splitConfig.getSplitField());
            return getFixedSplitSource(jdbcTableHandle);
        }
        long timeStamp = System.nanoTime();
        if (splitConfig.isCalcStepEnable()) {
            List<SplitStatLog> splitLogs;
            try {
                splitLogs = stepCalcManager.getAdjustSplitList(jdbcTableHandle.getCatalogName(), jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName());
                if (null == splitLogs) {
                    splitLogs = new ArrayList<SplitStatLog>();
                }
            }
            catch (PrestoException e) {
                log.error("Get table split from dynamic step calc manager failed, error info:" + e.getMessage());
                return getFixedSplitSource(jdbcTableHandle);
            }

            for (SplitStatLog splitLog : splitLogs) {
                String splitPart = splitConfig.getSplitField() + " > " + splitLog.getBeginIndex() + " and "
                        + splitConfig.getSplitField() + " <= " + splitLog.getEndIndex();
                JdbcSplit jdbcSplit = new JdbcSplit(splitLog.getCatalogName(),
                        splitLog.getSchemaName(),
                        splitLog.getTableName(),
                        splitConfig.getSplitField(),
                        splitLog.getBeginIndex().toString(),
                        splitLog.getEndIndex().toString(),
                        timeStamp,
                        splitConfig.getSplitCount(),
                        Optional.of(splitPart));
                jdbcSplitsList.add(jdbcSplit);
            }
        }

        if ((jdbcSplitsList == null) || (jdbcSplitsList.isEmpty())) {
            Long[] fieldMinAndMaxValue = new Long[2];
            try (Connection connection = jdbcClient.getConnection(identity, (JdbcSplit) null)) {
                fieldMinAndMaxValue = jdbcClient.getSplitFieldMinAndMaxValue(splitConfig, connection, jdbcTableHandle);
                if (fieldMinAndMaxValue == null || fieldMinAndMaxValue[0].equals(fieldMinAndMaxValue[1])) {
                    return getFixedSplitSource(jdbcTableHandle);
                }
            }
            catch (SQLException e) {
                return getFixedSplitSource(jdbcTableHandle);
            }
            splitTable(fieldMinAndMaxValue, jdbcTableHandle, jdbcSplitsList, splitConfig, timeStamp);
        }

        //submit static log, and filter fixed split source
        if (splitConfig.isCalcStepEnable() && (jdbcSplitsList.size() > 1)) {
            Thread collectStaticLogs = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    handleSplitStatic(identity, jdbcSplitsList);
                }
            });
            collectStaticLogs.setName("splitStatic");
            collectStaticLogs.setUncaughtExceptionHandler(
                    (tr, ex) -> System.out.println(tr.getName() + ":" + ex.getMessage()));
            collectStaticLogs.start();
        }
        return new DataSourceSplitSource(jdbcSplitsList);
    }

    private FixedSplitSource getFixedSplitSource(JdbcTableHandle tableHandle)
    {
        return new FixedSplitSource(ImmutableList.of(new JdbcSplit(tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                "", "", "",
                System.nanoTime(), 1,
                Optional.empty())));
    }

    private void addJdbcSplit(JdbcTableHandle tableHandle,
            List<JdbcSplit> builder, String[] splitInfo, long timeStamp, int scanNodes, TableSplitConfig config)
    {
        JdbcSplit jdbcSplit = new JdbcSplit(tableHandle.getCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName(),
                config.getSplitField(), splitInfo[1], splitInfo[2], timeStamp, scanNodes,
                Optional.of(splitInfo[0]));
        builder.add(jdbcSplit);
    }

    /**
     * Splitting table by field or limit
     */
    private void splitTable(Long[] fieldMinAndMaxValue, JdbcTableHandle jdbcTableHandle,
            List<JdbcSplit> splits, TableSplitConfig config, long timeStamp)
    {
        long tableTotalRecords;
        int scanNodes = config.getSplitCount();
        tableTotalRecords = fieldMinAndMaxValue[0] - fieldMinAndMaxValue[1] + 1;
        long targetChunkSize = (long) Math.ceil(tableTotalRecords * 1.0 / scanNodes);
        long chunkOffset = 0L;
        long autoIncrementOffset = 0L;
        while (chunkOffset < tableTotalRecords) {
            long chunkLength = Math.min(targetChunkSize, tableTotalRecords - chunkOffset);
            if (chunkOffset == 0) {
                autoIncrementOffset = fieldMinAndMaxValue[1];
            }
            String[] splitInfo = getSplitInfo(chunkOffset, autoIncrementOffset - 1,
                    fieldMinAndMaxValue, chunkLength, tableTotalRecords, config.getSplitField());
            addJdbcSplit(jdbcTableHandle, splits, splitInfo, timeStamp, scanNodes, config);
            chunkOffset += chunkLength;
            autoIncrementOffset += chunkLength;
        }
        if (!config.isDataReadOnly()) {
            fillLastRecord(jdbcTableHandle, splits, scanNodes, timeStamp, fieldMinAndMaxValue[0], config);
            fillFirstRecord(jdbcTableHandle, splits, scanNodes, timeStamp, fieldMinAndMaxValue[1] - 1, config);
        }
    }

    private void fillLastRecord(JdbcTableHandle jdbcTableHandle, List<JdbcSplit> splits, int scanNodes, long timeStamp, long endIndex, TableSplitConfig config)
    {
        String splitPart = config.getSplitField() + " > " + endIndex;
        addJdbcSplit(jdbcTableHandle, splits,
                new String[] {splitPart, String.valueOf(endIndex), String.valueOf(Long.MAX_VALUE)}, timeStamp, scanNodes, config);
    }

    private void fillFirstRecord(JdbcTableHandle jdbcTableHandle, List<JdbcSplit> splits, int scanNodes, long timeStamp, long startIndex, TableSplitConfig config)
    {
        String splitPart = config.getSplitField() + " <= " + startIndex;
        addJdbcSplit(jdbcTableHandle, splits,
                new String[] {splitPart, String.valueOf(Long.MIN_VALUE), String.valueOf(startIndex)}, timeStamp, scanNodes, config);
    }

    /**
     * If the table split by field,the filter conditions will follow like this :
     * field > offset and field <= offset + chunkLength.
     *
     * @return splitInfo[0] : splitPart; splitInfo[1] : beginIndex; splitInfo[2] : endIndex;
     */
    private String[] getSplitInfo(long chunkOffset, long autoIncrementOffset,
            Long[] splitFieldRangeValue, long chunkLength, long tableTotalRecords, String splitField)
    {
        String[] splitInfo = new String[3];
        String splitPart;
        splitInfo[1] = String.valueOf(autoIncrementOffset);
        splitPart = splitField + " > " + autoIncrementOffset + " and " + splitField + " <= ";
        if ((chunkOffset + chunkLength) == tableTotalRecords) {
            splitPart += splitFieldRangeValue[0];
            splitInfo[2] = String.valueOf(splitFieldRangeValue[0]);
        }
        else {
            splitPart += (autoIncrementOffset + chunkLength);
            splitInfo[2] = String.valueOf(autoIncrementOffset + chunkLength);
        }
        splitInfo[0] = splitPart;
        return splitInfo;
    }
}
