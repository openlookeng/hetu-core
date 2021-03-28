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
package io.prestosql.statestore;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.SessionRepresentation;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.dispatcher.LocalDispatchQuery;
import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.execution.StateMachine;
import io.prestosql.operator.BlockedReason;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.Returns;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.ErrorType.USER_ERROR;
import static io.prestosql.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * Test for StateUpdater class
 *
 * @since 2019-11-29
 */
@Test(singleThreaded = true)
public class TestStateUpdater
{
    private static final int MINIMUM_UPDATE_INTERVAL = 10;
    private static final String STATE_COLLECTION_QUERY = "query";
    private static final String DISPATCH_QUERY_ENTRY = "dispatchquery";
    private static final String NULL_STRING = null;
    private static final String MOCK_QUERY_ID = "20191122_174317_00000_q9aun";
    private static final String URI_LOCALHOST = "http://localhost:8080";
    private static final String GLOBAL_RESOURCE_ID = "global";
    private static final String QUERY_STRING = "select * from tpch.tiny.customer";
    private static final String PREPARED_QUERY_MOCK_DATA = "prep query mock data";
    private static final String ERROR_CODE_TEST = "test";
    private static final int ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION = 0;
    private static final Double TOTAL_DATA_SIZE = 10000000D;
    private static final Double USER_DATA_SIZE = 1000000D;
    private Duration updateInterval;
    private StateStore stateStore;

    @BeforeMethod
    public void setUp()
    {
        updateInterval = new Duration(MINIMUM_UPDATE_INTERVAL, MILLISECONDS);
        stateStore = Mockito.mock(StateStore.class);
    }

    @AfterMethod
    public void tearDown()
    {
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        stateUpdater.stop();
    }

    @Test
    public void testStart()
    {
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        stateUpdater.start();
        assertEquals(stateStoreProvider.getStateStore(), NULL_STRING);
    }

    @Test
    public void testStop()
    {
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        stateUpdater.stop();
    }

    @Test
    public void testRegisterQuery()
    {
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        DispatchQuery dispatchQuery = Mockito.mock(DispatchQuery.class);
        stateUpdater.registerQuery(STATE_COLLECTION_QUERY, dispatchQuery);
    }

    @Test
    public void testUnregisterQuery()
    {
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        ManagedQueryExecution managedQueryExecution = Mockito.mock(ManagedQueryExecution.class);
        stateUpdater.unregisterQuery(STATE_COLLECTION_QUERY, managedQueryExecution);
    }

    private BasicQueryInfo createBasicQueryInfo()
    {
        QueryInfo queryInfo = Mockito.mock(QueryInfo.class);
        when(queryInfo.getQueryStats()).then(new Returns(Mockito.mock(QueryStats.class)));
        Duration mockInterval = new Duration(MINIMUM_UPDATE_INTERVAL, MILLISECONDS);
        when(queryInfo.getQueryStats().getQueuedTime()).then(new Returns(mockInterval));
        when(queryInfo.getQueryStats().getElapsedTime()).then(new Returns(mockInterval));
        when(queryInfo.getQueryStats().getExecutionTime()).then(new Returns(mockInterval));
        when(queryInfo.getQueryStats().getRawInputDataSize()).then(new Returns(Mockito.mock(DataSize.class)));
        String mockQueryId = MOCK_QUERY_ID;
        QueryId queryId = new QueryId(mockQueryId);
        when(queryInfo.getQueryId()).then(new Returns(queryId));
        SessionRepresentation sessionRepresentation = TEST_SESSION.toSessionRepresentation();
        when(queryInfo.getSession()).then(new Returns(sessionRepresentation));
        ResourceGroupId resourceGroupId = new ResourceGroupId(GLOBAL_RESOURCE_ID);
        Optional<ResourceGroupId> optionalResourceGroupId = Optional.of(resourceGroupId);
        when(queryInfo.getResourceGroupId()).then(new Returns(optionalResourceGroupId));
        when(queryInfo.getState()).then(new Returns(QueryState.FINISHED));
        URI mockURI = URI.create(URI_LOCALHOST);
        when(queryInfo.getSelf()).then(new Returns(mockURI));
        String mockQuery = QUERY_STRING;
        when(queryInfo.getQuery()).then(new Returns(mockQuery));
        Optional<String> preparedQuery = Optional.of(PREPARED_QUERY_MOCK_DATA);
        when(queryInfo.getPreparedQuery()).then(new Returns(preparedQuery));
        ErrorCode errorCode = new ErrorCode(ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION, ERROR_CODE_TEST, USER_ERROR);
        when(queryInfo.getErrorCode()).then(new Returns(errorCode));
        Set<BlockedReason> setBlockedReason = new HashSet<>();
        setBlockedReason.add(BlockedReason.WAITING_FOR_MEMORY);
        when(queryInfo.getQueryStats().getBlockedReasons()).then(new Returns(setBlockedReason));
        when(queryInfo.getQueryStats().getProgressPercentage()).then(new Returns(OptionalDouble.empty()));
        BasicQueryInfo basicQueryInfo = new BasicQueryInfo(queryInfo);
        return basicQueryInfo;
    }

    private DispatchQuery mockDispatchQueryData(boolean userError)
    {
        DispatchQuery dispatchQuery = Mockito.mock(LocalDispatchQuery.class);
        BasicQueryInfo basicQueryInfo = createBasicQueryInfo();
        when(dispatchQuery.getBasicQueryInfo()).then(new Returns(basicQueryInfo));
        when(dispatchQuery.getSession()).then(new Returns(TEST_SESSION));
        ErrorCode errorCode;
        if (!userError) {
            errorCode = CLUSTER_OUT_OF_MEMORY.toErrorCode();
        }
        else {
            errorCode = new ErrorCode(ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION, ERROR_CODE_TEST, USER_ERROR);
        }
        Optional<ErrorCode> optionalErrorCode = Optional.of(errorCode);
        when(dispatchQuery.getErrorCode()).then(new Returns(optionalErrorCode));
        DataSize userDataSize = new DataSize(USER_DATA_SIZE, DataSize.Unit.BYTE);
        DataSize totalDataSize = new DataSize(TOTAL_DATA_SIZE, DataSize.Unit.BYTE);
        when(dispatchQuery.getUserMemoryReservation()).then(new Returns(userDataSize));
        when(dispatchQuery.getTotalMemoryReservation()).then(new Returns(totalDataSize));
        when(dispatchQuery.getTotalCpuTime()).then(new Returns(new Duration(ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION, MILLISECONDS)));
        when(dispatchQuery.getExecutionStartTime()).then(new Returns(Optional.of(new DateTime(DateTimeZone.UTC))));
        return dispatchQuery;
    }

    private void updateStateChange(DispatchQuery dispatchQuery)
    {
        doAnswer((i) -> {
            StateMachine.StateChangeListener stateChangeListener = (StateMachine.StateChangeListener) i.getArguments()[ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION];
            stateChangeListener.stateChanged(QueryState.FINISHED);
            return NULL_STRING;
        }).when(dispatchQuery).addStateChangeListener(any());
    }

    @Test
    public void testUpdateStates() throws JsonProcessingException
    {
        DispatchQuery dispatchQuery = mockDispatchQueryData(false);
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        stateUpdater.registerQuery(STATE_COLLECTION_QUERY, dispatchQuery);
        when(stateStoreProvider.getStateStore()).then(new Returns(stateStore));
        when(stateStoreProvider.getStateStore().getStateCollection(any())).then(new Returns(Mockito.mock(StateMap.class)));
        when(stateStoreProvider.getStateStore().getStateCollection(any()).getType()).then(new Returns(StateCollection.Type.MAP));
        updateStateChange(dispatchQuery);
        stateUpdater.updateStates();
        int numberOfCalls = mockingDetails(stateStoreProvider.getStateStore().getStateCollection(any())).getInvocations().size();
        assertNotEquals(numberOfCalls, ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION);
        stateUpdater.registerQuery(STATE_COLLECTION_QUERY, dispatchQuery);
    }

    @Test
    public void testStateChangeListenerOfRegisterQuery() throws JsonProcessingException
    {
        StateStoreProvider stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        StateUpdater stateUpdater = new StateUpdater(stateStoreProvider, updateInterval);
        DispatchQuery dispatchQuery = mockDispatchQueryData(true);
        updateStateChange(dispatchQuery);
        stateUpdater.registerQuery(DISPATCH_QUERY_ENTRY, dispatchQuery);
        when(stateStoreProvider.getStateStore()).then(new Returns(stateStore));
        when(stateStoreProvider.getStateStore().getStateCollection(any())).then(new Returns(Mockito.mock(StateMap.class)));
        when(stateStoreProvider.getStateStore().getStateCollection(any()).getType()).then(new Returns(StateCollection.Type.MAP));
        stateUpdater.updateStates();
        int numberOfCalls = mockingDetails(stateStoreProvider.getStateStore().getStateCollection(any())).getInvocations().size();
        assertNotEquals(numberOfCalls, ERROR_CODE_VALUE_INDEX_TIME_NO_INVOCATION);
    }
}
