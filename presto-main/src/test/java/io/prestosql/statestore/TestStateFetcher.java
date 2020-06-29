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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryState;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.statestore.StateStoreFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.Returns;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Test StateFetcher
 *
 * @since 2019-11-29
 */
@Test(singleThreaded = true)
public class TestStateFetcher
{
    private StateFetcher stateFetcher;
    private StateStoreProvider stateStoreProvider;
    private StateMap stateCollection;
    private StateStoreFactory stateStoreFactory;
    private StateStore stateStore;
    private static final String STATES_KEY = "20191120_160018_00000_qtvps";
    private static final String STATE_STORE_HAZELCAST = "hazelcast";
    private static final String STATE_COLLECTION_QUERY = "query";
    private static final int MINIMUM_FETCH_INTERVAL = 1;
    private static final int MINIMUM_STATE_EXPIRE_TIME = 500;
    private static final String EMPTY_JSON_STRING = "{}";
    private static final String MOCK_TEST_DATA_RESOURCE_NAME = "test_data_state_fetcher.json";
    private static final int CACHED_STATES_MAP_SIZE = 1;
    private static final int CACHED_STATES_EMPTY_BEGIN_INDEX_NO_INVOCATION = 0;
    private static final int DATE_TIME_STRING_LENGTH = 43;
    private static final String QUERY_STATE_PLANNING = "PLANNING";
    private static final String QUERY_SELF_INFO_MOCK_DATA = "http://127.0.0.1:8080/v1/query/20191120_160018_00000_qtvps";
    private static final String QUERY_STRING_MOCK_DATA = "select * from tpch.tiny.customer";
    private static final String STATE_UPDATE_TIME_KEY = "\"stateUpdateTime\":\"";
    private static final String STRING_NULL = null;
    private Duration fetchInterval = new Duration(MINIMUM_FETCH_INTERVAL, MILLISECONDS);
    private Duration stateExpireTime = new Duration(MINIMUM_STATE_EXPIRE_TIME, SECONDS);

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        stateStoreProvider = Mockito.mock(LocalStateStoreProvider.class);
        stateFetcher = new StateFetcher(stateStoreProvider, fetchInterval, stateExpireTime);
    }

    private void stateStoreMockData()
    {
        stateFetcher.registerStateCollection(STATE_COLLECTION_QUERY);
        stateStoreFactory = Mockito.mock(StateStoreFactory.class);
        when(stateStoreFactory.getName()).then(new Returns(STATE_STORE_HAZELCAST));
        stateStore = Mockito.mock(StateStore.class);
        when(stateStore.getName()).then(new Returns(STATE_COLLECTION_QUERY));
        when(stateStoreFactory.create(any(), any(), any())).then(new Returns(stateStore));
        stateCollection = Mockito.mock(StateMap.class);
        when(stateStoreProvider.getStateStore()).then(new Returns(stateStore));
    }

    private void mockStateCollectionData()
    {
        when(stateStoreProvider.getStateStore().getStateCollection(any())).then(new Returns(stateCollection));
    }

    private void mockStateCollectionEmptyData()
    {
        when(stateStoreProvider.getStateStore()).then(new Returns(stateStore));
        when(stateStoreProvider.getStateStore().getStateCollection(any())).then(new Returns(STRING_NULL));
    }

    private void supportCollectionTypeMAP(boolean setCurrentTime)
            throws Exception
    {
        stateStoreMockData();
        mockStateCollectionData();
        when(stateCollection.getType()).then(new Returns(StateCollection.Type.MAP));
        Map<String, String> states = new ConcurrentHashMap<>();
        String statesKey = STATES_KEY;
        String mockDataPath = this.getClass().getClassLoader().getResource(MOCK_TEST_DATA_RESOURCE_NAME).getPath();
        File fileStateStoreMockData = new File(mockDataPath);
        String statesValue = loadMockTestData(fileStateStoreMockData, setCurrentTime);
        states.put(statesKey, statesValue);
        when(stateCollection.getAll()).then(new Returns(states));
        stateStoreProvider.addStateStoreFactory(stateStoreFactory);
        stateStoreProvider.loadStateStore();
    }

    private void supportCollectionWithInvalidStates()
    {
        stateStoreMockData();
        mockStateCollectionData();
        when(stateCollection.getType()).then(new Returns(StateCollection.Type.MAP));
        Map<String, String> states = new ConcurrentHashMap<>();
        String statesKey = STATES_KEY;
        String statesValue = EMPTY_JSON_STRING;
        states.put(statesKey, statesValue);
        when(stateCollection.getAll()).then(new Returns(states));
    }

    private String loadMockTestData(File file, boolean useCurrentTime)
            throws IOException
    {
        String mockData;
        try (InputStream in = new FileInputStream(file)) {
            InputStreamReader isReader = new InputStreamReader(in);
            BufferedReader reader = new BufferedReader(isReader);
            StringBuilder stringBuilder = new StringBuilder();
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                stringBuilder.append(tempString);
            }
            mockData = stringBuilder.toString();
        }
        if (useCurrentTime) {
            int lastIndex = mockData.lastIndexOf(STATE_UPDATE_TIME_KEY);
            String prefixMockData = mockData.substring(CACHED_STATES_EMPTY_BEGIN_INDEX_NO_INVOCATION, lastIndex);
            String suffixMockData = mockData.substring(lastIndex + DATE_TIME_STRING_LENGTH, mockData.length());
            DateTime dateTime = new DateTime(DateTimeZone.UTC);
            mockData = prefixMockData + STATE_UPDATE_TIME_KEY + dateTime.toString() + suffixMockData;
        }
        return mockData;
    }

    @Test
    public void testStartStop()
            throws InterruptedException
    {
        stateFetcher.start();

        Thread.sleep(9000);

        stateFetcher.stop();
    }

    @Test
    public void testRegisterUnregisterStateCollection()
    {
        stateFetcher.registerStateCollection(STATE_COLLECTION_QUERY);
        stateFetcher.unregisterStateCollection(STATE_COLLECTION_QUERY);
    }

    @Test
    public void testFetchStatesWithCollectionTypeSupport()
            throws Exception
    {
        supportCollectionTypeMAP(true);
        stateFetcher.start();
        stateFetcher.fetchStates();
        assertEquals(StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY).size(), CACHED_STATES_MAP_SIZE);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testFetchStatesWithInvalidStates()
            throws Exception
    {
        supportCollectionWithInvalidStates();
        stateFetcher.fetchStates();
        assertEquals(StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY).size(), CACHED_STATES_MAP_SIZE);
    }

    @Test
    public void testFetchStatesWithEmptyStateStore()
            throws Exception
    {
        stateStoreMockData();
        mockStateCollectionData();
        when(stateStoreProvider.getStateStore()).then(new Returns(STRING_NULL));
        stateFetcher.fetchStates();
        assertEquals(StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY).size(), CACHED_STATES_MAP_SIZE);
    }

    @Test
    public void testFetchStatesWithEmptyCollection()
            throws Exception
    {
        stateStoreMockData();
        mockStateCollectionEmptyData();
        stateFetcher.fetchStates();
        assertEquals(StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY).size(), CACHED_STATES_MAP_SIZE);
    }

    @Test
    public void testStateQueryInfoId()
            throws IOException
    {
        String mockDataPath = this.getClass().getClassLoader().getResource(MOCK_TEST_DATA_RESOURCE_NAME).getPath();
        File fileStateStoreMockData = new File(mockDataPath);
        String statesValue = loadMockTestData(fileStateStoreMockData, true);
        ObjectMapper mapper = new ObjectMapperProvider().get();
        SharedQueryState state = mapper.readerFor(SharedQueryState.class).readValue(statesValue);
        assertEquals(state.getBasicQueryInfo().getQueryId().getId(), STATES_KEY);
        assertEquals(state.getBasicQueryInfo().getState().toString(), QUERY_STATE_PLANNING);
        assertEquals(state.getBasicQueryInfo().getSelf().toString(), QUERY_SELF_INFO_MOCK_DATA);
        assertEquals(state.getBasicQueryInfo().getQuery(), QUERY_STRING_MOCK_DATA);
        assertEquals(state.getBasicQueryInfo().getState(), QueryState.PLANNING);
    }

    @Test
    public void testStateStoreCollectionData()
    {
        mockStateCollectionEmptyData();
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(STATE_COLLECTION_QUERY), STRING_NULL);
    }

    @Test
    public void testCachedStates()
            throws Exception
    {
        supportCollectionTypeMAP(true);
        stateFetcher.fetchStates();
        assertEquals(StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY).size(), CACHED_STATES_MAP_SIZE);
    }

    @Test
    public void testFetchStatesForExpiredState()
            throws Exception
    {
        supportCollectionTypeMAP(false);
        stateFetcher.fetchStates();
        Map stateCacheStore = StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY);
        assertEquals(StateCacheStore.get().getCachedStates(STATE_COLLECTION_QUERY).size(), CACHED_STATES_MAP_SIZE);
        //TODO check state change
    }

    @Test
    public void testgetAllCallsAfterStart()
            throws Exception
    {
        supportCollectionTypeMAP(true);
        stateFetcher.start();
        Thread.sleep(300);
        verify(stateCollection, atLeastOnce()).getAll();
    }
}
