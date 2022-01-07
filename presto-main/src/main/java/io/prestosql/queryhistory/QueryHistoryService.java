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
package io.prestosql.queryhistory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
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
import io.prestosql.spi.queryhistory.QueryHistoryEntity;
import io.prestosql.spi.queryhistory.QueryHistoryResult;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryHistoryService
{
    private final HetuMetaStoreManager hetuMetaStoreManager;

    private static final DataSize ZERO_BYTES = new DataSize(0, DataSize.Unit.BYTE);
    private static final Duration ZERO_MILLIS = new Duration(0, TimeUnit.MILLISECONDS);
    private static AtomicLong currentQueries = new AtomicLong(0);

    @Inject
    public QueryHistoryService(HetuMetaStoreManager hetuMetaStoreManager)
    {
        this.hetuMetaStoreManager = requireNonNull(hetuMetaStoreManager, "metaStoreManager is null");
    }

    public void insert(QueryInfo queryInfo)
    {
        if (queryInfo == null) {
            return;
        }
        try {
            String user = queryInfo.getSession().getUser();
            String source = queryInfo.getSession().getSource().get();
            if (source == null) {
                return;
            }
            String queryId = queryInfo.getQueryId().getId();
            String resource = null;
            if (queryInfo.getResourceGroupId().isPresent()) {
                resource = queryInfo.getResourceGroupId().get().getLastSegment();
            }
            String query = queryInfo.getQuery();
            String catalog = queryInfo.getSession().getCatalog().get();
            String schemata = queryInfo.getSession().getSchema().get();
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
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(queryInfo);
            int completedDrivers = queryInfo.getQueryStats().getCompletedDrivers();
            int runningDrivers = queryInfo.getQueryStats().getRunningDrivers();
            int queuedDrivers = queryInfo.getQueryStats().getQueuedDrivers();
            String totalCpuTime = queryInfo.getQueryStats().getTotalCpuTime().toString();
            String totalMemoryReservation = queryInfo.getQueryStats().getTotalMemoryReservation().toString();
            String peakTotalMemoryReservation = queryInfo.getQueryStats().getPeakTotalMemoryReservation().toString();

            QueryHistoryEntity queryHistoryEntity = new QueryHistoryEntity();
            queryHistoryEntity.setQueryId(queryId);
            queryHistoryEntity.setState(state);
            queryHistoryEntity.setFailed(failed);
            queryHistoryEntity.setQuery(query);
            queryHistoryEntity.setUser(user);
            queryHistoryEntity.setSource(source);
            queryHistoryEntity.setResource(resource);
            queryHistoryEntity.setCatalog(catalog);
            queryHistoryEntity.setSchemata(schemata);
            queryHistoryEntity.setCurrentMemory(currentMemory);
            queryHistoryEntity.setCompletedDrivers(completedDrivers);
            queryHistoryEntity.setRunningDrivers(runningDrivers);
            queryHistoryEntity.setQueuedDrivers(queuedDrivers);
            queryHistoryEntity.setCreateTime(createTime);
            queryHistoryEntity.setExecutionTime(executionTime);
            queryHistoryEntity.setElapsedTime(elapsedTime);
            queryHistoryEntity.setCpuTime(cpuTime);
            queryHistoryEntity.setTotalCpuTime(totalCpuTime);
            queryHistoryEntity.setTotalMemoryReservation(totalMemoryReservation);
            queryHistoryEntity.setPeakTotalMemoryReservation(peakTotalMemoryReservation);
            queryHistoryEntity.setCumulativeUserMemory(cumulativeUserMemory);
            hetuMetaStoreManager.getHetuMetastore().insertQueryHistory(queryHistoryEntity, jsonString);
            currentQueries.incrementAndGet();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getQueryDetail(String queryId)
    {
        String res = hetuMetaStoreManager.getHetuMetastore().getQueryDetail(queryId);
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
        if (sort.equals("cumulative")) {
            sort = "cumulativeUserMemory";
        }
        else if (sort.equals("memory")) {
            sort = "currentMemory";
        }
        else if (sort.equals("elapsed")) {
            sort = "elapsedTime";
        }
        else if (sort.equals("cpu")) {
            sort = "cpuTime";
        }
        else if (sort.equals("execution")) {
            sort = "executionTime";
        }
        else {
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
            stateFilter.add(s.toUpperCase());
        }
        stateFilter.add("null");

        for (String s : failedStr) {
            failFilter.add(s.toUpperCase());
        }
        failFilter.add("null");
        QueryHistoryResult queryHistoryResult = hetuMetaStoreManager.getHetuMetastore().getQueryHistory(startNum, pageSize, user, startTime, endTime,
                queryId, query, resource, source, stateFilter, failFilter, sort, sortOrder);

        return queryHistoryResult;
    }

    public static Long getCurrentQueries()
    {
        return currentQueries.get();
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
                false,
                ImmutableSet.of(),
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                ImmutableList.of(),
                ImmutableList.of());

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
                info.getResourceGroupId(), false);
    }
}
