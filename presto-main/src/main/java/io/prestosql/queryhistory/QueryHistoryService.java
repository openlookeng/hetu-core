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
package io.prestosql.queryhistory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryStats;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.protocol.ObjectMapperProvider;
import io.prestosql.queryhistory.model.Info;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.queryhistory.QueryHistoryEntity;
import io.prestosql.spi.queryhistory.QueryHistoryResult;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryHistoryService
{
    private static final Logger log = Logger.get(QueryHistoryService.class);
    private final HetuMetaStoreManager hetuMetaStoreManager;
    private final QueryHistoryConfig queryHistoryConfig;
    private HetuMetastore hetuMetastore;

    private static final DataSize ZERO_BYTES = new DataSize(0, DataSize.Unit.BYTE);
    private static final Duration ZERO_MILLIS = new Duration(0, TimeUnit.MILLISECONDS);
    private static AtomicLong currentQueries = new AtomicLong(0);

    @Inject
    public QueryHistoryService(HetuMetaStoreManager hetuMetaStoreManager, QueryHistoryConfig queryHistoryConfig)
    {
        this.hetuMetaStoreManager = requireNonNull(hetuMetaStoreManager, "metaStoreManager is null");
        this.queryHistoryConfig = requireNonNull(queryHistoryConfig, "queryHistoryConfig is null");
        this.hetuMetastore = validateMetaStore();
    }

    private HetuMetastore validateMetaStore()
    {
        if (hetuMetaStoreManager.getHetuMetastore() == null) {
            this.reviseHetuMetastore();
            return MockhetuMetaStore.getInstance();
        }
        return hetuMetaStoreManager.getHetuMetastore();
    }

    /**
     * reviseHetuMetastore
     */
    private void reviseHetuMetastore()
    {
        new Thread(() -> {
            while (hetuMetaStoreManager.getHetuMetastore() == null) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                }
                catch (InterruptedException e) {
                    log.error(e, "Error reviseHetuMetastore");
                }
            }

            this.hetuMetastore = validateMetaStore();
        }).start();
    }

    public void insert(QueryInfo queryInfo)
    {
        if (queryInfo == null) {
            return;
        }
        String user = queryInfo.getSession().getUser();
        String source = queryInfo.getSession().getSource().orElse(null);
        if (source == null) {
            return;
        }
        String queryId = queryInfo.getQueryId().getId();
        String resource = null;
        if (queryInfo.getResourceGroupId().isPresent()) {
            resource = queryInfo.getResourceGroupId().get().getLastSegment();
        }
        String query = queryInfo.getQuery();
        String catalog = queryInfo.getSession().getCatalog().orElse("");
        String schemata = queryInfo.getSession().getSchema().orElse("");
        String state = queryInfo.getState().toString();
        String failed = "null";
        if (queryInfo.getErrorType() != null) {
            failed = queryInfo.getErrorType().toString();
        }
        String createTime = queryInfo.getQueryStats().getCreateTime().toString();
        String elapsedTime = queryInfo.getQueryStats().getElapsedTime().toString();
        String cpuTime = queryInfo.getQueryStats().getTotalCpuTime().toString();
        String executionTime = queryInfo.getQueryStats().getExecutionTime().toString();
        String currentMemory = queryInfo.getQueryStats().getUserMemoryReservation().toString();
        double cumulativeUserMemory = queryInfo.getQueryStats().getCumulativeUserMemory();
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        String jsonString = null;
        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(queryInfo);
        }
        catch (JsonProcessingException e) {
            log.error(e);
        }
        int completedDrivers = queryInfo.getQueryStats().getCompletedDrivers();
        int runningDrivers = queryInfo.getQueryStats().getRunningDrivers();
        int queuedDrivers = queryInfo.getQueryStats().getQueuedDrivers();
        String totalCpuTime = queryInfo.getQueryStats().getTotalCpuTime().toString();
        String totalMemoryReservation = queryInfo.getQueryStats().getTotalMemoryReservation().toString();
        String peakTotalMemoryReservation = queryInfo.getQueryStats().getPeakTotalMemoryReservation().toString();

        QueryHistoryEntity queryHistoryEntity = new QueryHistoryEntity.Builder()
                .setQueryId(queryId)
                .setState(state)
                .setFailed(failed)
                .setQuery(query)
                .setUser(user)
                .setSource(source)
                .setResource(resource)
                .setCatalog(catalog)
                .setSchemata(schemata)
                .setCurrentMemory(currentMemory)
                .setCompletedDrivers(completedDrivers)
                .setRunningDrivers(runningDrivers)
                .setQueuedDrivers(queuedDrivers)
                .setCreateTime(createTime)
                .setExecutionTime(executionTime)
                .setElapsedTime(elapsedTime)
                .setCpuTime(cpuTime)
                .setTotalCpuTime(totalCpuTime)
                .setTotalMemoryReservation(totalMemoryReservation)
                .setPeakTotalMemoryReservation(peakTotalMemoryReservation)
                .setCumulativeUserMemory(cumulativeUserMemory)
                .build();
        hetuMetastore.insertQueryHistory(queryHistoryEntity, jsonString);
        currentQueries.incrementAndGet();
    }

    public String getQueryDetail(String queryId)
    {
        String res = hetuMetastore.getQueryDetail(queryId);
        return res;
    }

    public QueryHistoryResult Query(Info info, int pageNum, int pageSize)
    {
        String user = info.getUser();
        String startTime = info.getStartTime();
        String endTime = info.getEndTime();
        String query = info.getQuery();
        String queryId = info.getQueryId();
        String source = info.getSource();
        String resource = info.getResource();
        String state = info.getState();
        String failed = info.getFailed();
        String sort = info.getSort();
        String sortOrder = info.getSortorder();
        int startNum = (pageNum - 1) * pageSize;
        String[] stateStr = state.split(",");
        String[] failedStr = failed.split(",");
        if (sortOrder.equals("descending")) {
            sortOrder = "desc";
        }
        else {
            sortOrder = "asc";
        }
        switch (sort) {
            case "cumulative":
                sort = "cumulativeUserMemory";
                break;
            case "memory":
                sort = "currentMemory";
                break;
            case "elapsed":
                sort = "elapsedTime";
                break;
            case "cpu":
                sort = "cpuTime";
                break;
            case "execution":
                sort = "executionTime";
                break;
            default:
                sort = "createTime";
        }
        if (stateStr == null || stateStr.equals("")) {
            return new QueryHistoryResult(0, null);
        }

        if (!Strings.isNullOrEmpty(startTime)) {
            startTime = startTime.replace(".", "T");
        }
        if (!Strings.isNullOrEmpty(endTime)) {
            endTime = endTime.replace(".", "T");
        }

        ArrayList<String> stateFilter = new ArrayList<>();
        ArrayList<String> failFilter = new ArrayList<>();
        for (String s : stateStr) {
            stateFilter.add(s.toUpperCase(Locale.ENGLISH));
        }
        stateFilter.add("null");

        for (String s : failedStr) {
            failFilter.add(s.toUpperCase(Locale.ENGLISH));
        }
        failFilter.add("null");
        QueryHistoryResult queryHistoryResult = hetuMetastore.getQueryHistory(startNum, pageSize, user, startTime, endTime,
                queryId, query, resource, source, stateFilter, failFilter, sort, sortOrder);
        return queryHistoryResult;
    }

    // assign queryHistoryCount to currentQueries
    // if queryHistoryCount > 1000, then delete the oldest 100 queryHistory
    public Long getCurrentQueries()
    {
        if (currentQueries.get() == 0) {
            currentQueries.set(hetuMetastore.getAllQueryHistoryNum());
        }
        if (currentQueries.get() > queryHistoryConfig.getMaxQueryHistoryCount()) {
            hetuMetastore.deleteQueryHistoryBatch();
            currentQueries.set(0);
        }
        return currentQueries.get();
    }

    public String getHetuMetastoreType()
    {
        return hetuMetaStoreManager.getHetuMetastoreType();
    }

    public static QueryInfo toFullQueryInfo(DispatchQuery query)
    {
        checkArgument(query.isDone(), "query is not done");
        BasicQueryInfo info = query.getBasicQueryInfo();
        BasicQueryStats stats = info.getQueryStats();

        QueryStats queryStats = new QueryStats(
                query.getCreateTime(),
                query.getExecutionStartTime().orElse(null),
                query.getLastHeartbeat(),
                query.getEndTime().orElse(null),
                stats.getElapsedTime(),
                stats.getQueuedTime(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                info.isScheduled(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                false,
                ImmutableSet.of(),
                ZERO_BYTES,
                ZERO_BYTES,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                ImmutableList.of(),
                ImmutableList.of(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS);

        return new QueryInfo(
                info.getQueryId(),
                info.getSession(),
                info.getState(),
                info.getMemoryPool(),
                info.isScheduled(),
                info.getSelf(),
                ImmutableList.of(),
                info.getQuery(),
                info.getPreparedQuery(),
                queryStats,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.empty(),
                query.getDispatchInfo().getFailureInfo().orElse(null),
                info.getErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                true,
                info.getResourceGroupId(), false,
                false);
    }
}
