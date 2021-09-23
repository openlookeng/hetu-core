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
package io.prestosql.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.PageAssertions;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.testing.assertions.Assert;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.testing.TestingPagesSerdeFactory.TESTING_SERDE_FACTORY;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMultiInputSnapshotState
{
    private static final String source1 = "source1";
    private static final String source2 = "source2";
    private static final String source3 = "source3";
    private static final Page regularPage = new Page(1);
    private static final MarkerPage marker1 = MarkerPage.snapshotPage(1);
    private static final MarkerPage marker2 = MarkerPage.snapshotPage(2);
    private static final MarkerPage resume1 = MarkerPage.resumePage(1);
    private static final SnapshotStateId snapshotId1 = createSnapshotStateId(1);
    private static final SnapshotStateId snapshotId2 = createSnapshotStateId(2);

    private static SnapshotStateId createSnapshotStateId(long snapshotId)
    {
        return new SnapshotStateId(snapshotId, new TaskId("query", 1, 1));
    }

    private TaskSnapshotManager snapshotManager;
    private PagesSerde serde;
    private TestingRestorable restorable;
    private MultiInputSnapshotState state;

    private final ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);

    @BeforeMethod
    public void setup()
            throws Exception
    {
        serde = TESTING_SERDE_FACTORY.createPagesSerde();
        snapshotManager = mock(TaskSnapshotManager.class);
        restorable = new TestingRestorable();
        restorable.state = 100;
        state = new MultiInputSnapshotState(restorable, snapshotManager, serde, TestMultiInputSnapshotState::createSnapshotStateId);
    }

    private Optional<Page> processPage(String source, Page page)
    {
        return processPage(state, source, page);
    }

    private Optional<Page> processPage(MultiInputSnapshotState state, String source, Page page)
    {
        Optional<Page> ret = state.processPage(() -> Pair.of(page, page == null ? null : source));
        restorable.state++;
        return ret;
    }

    private Optional<Page> processPageKeepState(String source, Page page)
    {
        return state.processPage(() -> Pair.of(page, page == null ? null : source));
    }

    private Optional<SerializedPage> processSerializedPage(String source, SerializedPage page)
    {
        Optional<SerializedPage> ret = state.processSerializedPage(() -> Pair.of(page, page == null ? null : source));
        restorable.state++;
        return ret;
    }

    private Optional<SerializedPage> processSerializedPageKeepState(String source, SerializedPage page)
    {
        return state.processSerializedPage(() -> Pair.of(page, page == null ? null : source));
    }

    private List<Page> processPages(String source, List<Page> pages)
    {
        return state.processPages(pages, source);
    }

    private List<SerializedPage> processSerializedPages(String source, List<SerializedPage> pages)
    {
        return state.processSerializedPages(pages, source);
    }

    private boolean isRegularPage(Page page)
    {
        return page.getPositionCount() == 1 && page.getChannelCount() == 0;
    }

    @Test
    public void testRegularPage()
    {
        Page ret = processPage(source1, regularPage).get();
        assertEquals(ret, regularPage);
    }

    @Test
    public void testReturnSerializedPage()
    {
        SerializedPage ret = processSerializedPage(source1, serde.serialize(regularPage)).get();
        Page page = serde.deserialize(ret);
        assertTrue(isRegularPage(page));
    }

    @Test
    public void testMarkerPage()
            throws Exception
    {
        processPage(source1, regularPage);

        int saved = restorable.state;
        Optional<Page> ret = processPage(source1, marker1);
        assertEquals(ret.get(), marker1);

        processPage(source2, regularPage);
        processPage(source1, regularPage);

        ret = processPage(source2, marker1);
        assertFalse(ret.isPresent());

        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());
        List<Object> savedState = argument.getValue();
        assertEquals(savedState.size(), 2);
        assertEquals(savedState.get(0), saved);
        assertTrue(savedState.get(1).getClass().getName().contains("SerializedPageState"));
    }

    @Test
    public void testSerializedMarkerPage()
            throws Exception
    {
        processPage(source1, regularPage);

        int saved = restorable.state;
        SerializedPage serializedMarker = SerializedPage.forMarker(marker1);
        Optional<SerializedPage> ret = processSerializedPage(source1, serializedMarker);
        assertEquals(ret.get().toMarker(), marker1);

        processPage(source2, regularPage);
        processPage(source1, regularPage);

        ret = processSerializedPage(source2, serializedMarker);
        assertFalse(ret.isPresent());

        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());
        List<Object> savedState = argument.getValue();
        assertEquals(savedState.size(), 2);
        assertEquals(savedState.get(0), saved);
        assertTrue(savedState.get(1).getClass().getName().contains("SerializedPageState"));
    }

    @Test
    public void testMarkerPages()
            throws Exception
    {
        processPage(source1, regularPage);

        int saved1 = restorable.state;
        processPage(source1, marker1);

        int saved2 = restorable.state;
        Optional<Page> ret = processPage(source1, marker2);
        assertEquals(ret.get(), marker2);

        processPage(source2, regularPage);
        processPage(source2, marker1);

        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());
        List<Object> savedState = argument.getValue();
        assertEquals(savedState.size(), 2);
        assertEquals(savedState.get(0), saved1);
        assertTrue(savedState.get(1).getClass().getName().contains("SerializedPageState"));

        processPage(source2, regularPage);
        ret = processPage(source2, marker2);
        assertFalse(ret.isPresent());

        verify(snapshotManager).storeState(eq(snapshotId2), argument.capture());
        savedState = argument.getValue();
        assertEquals(savedState.size(), 3);
        assertEquals(savedState.get(0), saved2);
        assertTrue(savedState.get(1).getClass().getName().contains("SerializedPageState"));
        assertTrue(savedState.get(2).getClass().getName().contains("SerializedPageState"));
    }

    @Test
    public void testResume()
            throws Exception
    {
        processPage(source1, regularPage);

        int saved = restorable.state;
        processPage(source1, marker1);

        processPage(source2, regularPage);
        processPage(source2, regularPage);
        processPage(source2, marker1);
        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());

        when(snapshotManager.loadState(snapshotId1)).thenReturn(Optional.of(argument.getValue()));
        Optional<Page> ret = processPageKeepState(source2, resume1);
        assertEquals(ret.get(), resume1);

        assertEquals(restorable.state, saved);
        Assert.assertNotNull(processPageKeepState(null, null));
        Assert.assertNotNull(processSerializedPageKeepState(null, null));
        assertFalse(processPageKeepState(null, null).isPresent());
        assertFalse(processSerializedPageKeepState(null, null).isPresent());

        ret = processPage(source1, resume1);
        assertFalse(ret.isPresent());
    }

    @Test
    public void testUndeterminedSources()
            throws Exception
    {
        TestingRestorableUndeterminedInputs restorable = new TestingRestorableUndeterminedInputs();
        MultiInputSnapshotState state = new MultiInputSnapshotState(restorable, snapshotManager, serde, TestMultiInputSnapshotState::createSnapshotStateId);

        processPage(state, source1, marker1);
        processPage(state, source2, marker1);
        verify(snapshotManager, never()).storeState(anyObject(), anyObject());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUnexpectedSource()
    {
        processPage(source1, marker1);
        processPage(source3, marker2);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUnexpectedSourceMarker()
    {
        processPage(source3, marker1);
    }

    @Test
    public void testMarkerWhileResuming()
            throws Exception
    {
        processPage(source1, marker1);
        processPage(source2, marker1);
        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());

        when(snapshotManager.loadState(snapshotId1)).thenReturn(Optional.of(argument.getValue()));
        processPage(source1, resume1);

        Optional<Page> ret = processPage(source2, marker2);
        assertEquals(ret.get(), marker2);
    }

    @Test
    public void testPageWhileResuming()
            throws Exception
    {
        processPage(source1, marker1);
        processPage(source2, marker1);
        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());

        when(snapshotManager.loadState(snapshotId1)).thenReturn(Optional.of(argument.getValue()));
        processPage(source1, resume1);

        Optional<Page> ret = processPage(source1, regularPage);
        // Page is available even though the other channel has not received the restore marker.
        assertEquals(ret.get(), regularPage);

        processPage(source2, resume1);
        ret = processPage(source2, regularPage);
        assertEquals(ret.get(), regularPage);
    }

    @Test
    public void testPages()
    {
        List<Page> pages = processPages(source1, Arrays.asList(regularPage, regularPage));
        assertEquals(pages.size(), 2);
        assertEquals(pages.get(0), regularPage);
        assertEquals(pages.get(1), regularPage);
    }

    @Test
    public void testSerializedPages()
    {
        SerializedPage serializedPage = serde.serialize(regularPage);
        List<SerializedPage> pages = processSerializedPages(source1, Arrays.asList(serializedPage, serializedPage));
        assertEquals(pages.size(), 2);
        PageAssertions.assertPageEquals(ImmutableList.of(), serde.deserialize(pages.get(0)), regularPage);
        PageAssertions.assertPageEquals(ImmutableList.of(), serde.deserialize(pages.get(1)), regularPage);
    }

    @Test
    public void testPagesWithMarker()
            throws Exception
    {
        List<Page> pages = processPages(source1, Arrays.asList(marker1, regularPage));
        assertEquals(pages.size(), 2);
        assertEquals(pages.get(0), marker1);

        pages = processPages(source2, Arrays.asList(regularPage, marker1, regularPage));
        assertEquals(pages.size(), 2);
        assertEquals(pages.get(0), regularPage);
        assertEquals(pages.get(1), regularPage);

        verify(snapshotManager).storeState(eq(snapshotId1), argument.capture());
        when(snapshotManager.loadState(snapshotId1)).thenReturn(Optional.of(argument.getValue()));

        pages = processPages(source1, Arrays.asList(resume1));
        assertEquals(pages.size(), 2);
        assertEquals(pages.get(0), resume1);
        assertTrue(isRegularPage(pages.get(1)));
    }

    @Test
    public void testNextMarker()
            throws Exception
    {
        // No page
        Optional<Page> marker = state.nextMarker(() -> Pair.of(null, null));
        Assert.assertFalse(marker.isPresent());

        // No marker - page saved as pending
        marker = state.nextMarker(() -> Pair.of(regularPage, source1));
        Assert.assertFalse(marker.isPresent());
        marker = state.nextMarker(() -> Pair.of(null, null));
        Assert.assertFalse(marker.isPresent());
        Optional<Page> page = state.processPage(() -> Pair.of(null, null));
        assertTrue(page.isPresent());

        // Marker
        marker = state.nextMarker(() -> Pair.of(marker1, source1));
        assertTrue(marker.isPresent());

        // Resumed page
        when(snapshotManager.loadState(anyObject())).thenReturn(Optional.of(ImmutableList.of(0, serde.serialize(regularPage).capture(serde))));
        state.processPage(() -> Pair.of(resume1, source1));
        marker = state.nextMarker(() -> Pair.of(null, null));
        Assert.assertFalse(marker.isPresent());
        page = state.processPage(() -> Pair.of(null, null));
        assertTrue(page.isPresent());
    }

    @Test
    public void testNextSerializedMarker()
            throws Exception
    {
        // No page
        Optional<SerializedPage> marker = state.nextSerializedMarker(() -> Pair.of(null, null));
        Assert.assertFalse(marker.isPresent());

        // No marker - page saved as pending
        marker = state.nextSerializedMarker(() -> Pair.of(serde.serialize(regularPage), source1));
        Assert.assertFalse(marker.isPresent());
        marker = state.nextSerializedMarker(() -> Pair.of(null, null));
        Assert.assertFalse(marker.isPresent());
        Optional<SerializedPage> page = state.processSerializedPage(() -> Pair.of(null, null));
        assertTrue(page.isPresent());

        // Marker
        marker = state.nextSerializedMarker(() -> Pair.of(SerializedPage.forMarker(marker1), source1));
        assertTrue(marker.isPresent());

        // Resumed page
        when(snapshotManager.loadState(anyObject())).thenReturn(Optional.of(ImmutableList.of(0, serde.serialize(regularPage).capture(serde))));
        state.processPage(() -> Pair.of(resume1, source1));
        marker = state.nextSerializedMarker(() -> Pair.of(null, null));
        Assert.assertFalse(marker.isPresent());
        page = state.processSerializedPage(() -> Pair.of(null, null));
        assertTrue(page.isPresent());
    }

    @Test
    public void testStaticConstructor()
    {
        ScheduledExecutorService scheduler = newScheduledThreadPool(4);
        TaskContext taskContext = createTaskContext(scheduler, scheduler, TEST_SNAPSHOT_SESSION);
        DriverContext driverContext = taskContext
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("planNodeId"), "test");

        MultiInputSnapshotState state = MultiInputSnapshotState.forOperator(mock(MultiInputRestorable.class), operatorContext);
        processPage(state, source1, regularPage);

        state = MultiInputSnapshotState.forTaskComponent(mock(MultiInputRestorable.class), taskContext, TestMultiInputSnapshotState::createSnapshotStateId);
        processPage(state, source1, regularPage);
    }

    @Test
    public void testBroadcastedMarker()
            throws Exception
    {
        Page ret = processPage(null, marker1).get();
        assertEquals(ret, marker1);

        ret = processPage(null, resume1).get();
        assertEquals(ret, resume1);

        verify(snapshotManager, never()).storeState(anyObject(), anyObject());
        verify(snapshotManager, never()).loadState(anyObject());
    }

    @Test
    public void testStoreLoadConsolidatedSnapshot()
            throws Exception
    {
        // make sure to process each source from getInputChannels() in TestingRestorable
        // since store isn't called until every source has given the marker
        restorable.setSupportsConsolidatedWrites(true);
        processPage(state, source1, marker1);
        processPage(state, source2, marker1);
        verify(snapshotManager, never()).storeState(anyObject(), anyObject());
        verify(snapshotManager, times(1)).storeConsolidatedState(anyObject(), argument.capture());

        when(snapshotManager.loadConsolidatedState(anyObject())).thenReturn(Optional.of(argument.getValue()));
        processPage(state, source1, resume1);
        verify(snapshotManager, times(0)).loadState(anyObject());
        verify(snapshotManager, times(1)).loadConsolidatedState(anyObject());
    }

    @Test
    public void testStoreLoadComplexSnapshot()
            throws Exception
    {
        // make sure to process each source from getInputChannels() in TestingRestorable
        // since store isn't called until every source has given the marker
        restorable.setSupportsConsolidatedWrites(false);
        processPage(state, source1, marker1);
        processPage(state, source2, marker1);
        verify(snapshotManager, times(0)).storeConsolidatedState(anyObject(), anyObject());
        verify(snapshotManager, times(1)).storeState(anyObject(), argument.capture());

        when(snapshotManager.loadState(anyObject())).thenReturn(Optional.of(argument.getValue()));
        processPage(state, source1, resume1);
        verify(snapshotManager, times(0)).loadConsolidatedState(anyObject());
        verify(snapshotManager, times(1)).loadState(anyObject());
    }

    @RestorableConfig(uncapturedFields = {"supportsConsolidatedWrites"})
    private static class TestingRestorable
            implements MultiInputRestorable
    {
        int state;
        boolean supportsConsolidatedWrites;

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            return state;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            this.state = (Integer) state;
        }

        @Override
        public Optional<Set<String>> getInputChannels()
        {
            return Optional.of(Sets.newHashSet(source1, source2));
        }

        @Override
        public boolean supportsConsolidatedWrites()
        {
            return this.supportsConsolidatedWrites;
        }

        public void setSupportsConsolidatedWrites(boolean supportsConsolidatedWrites)
        {
            this.supportsConsolidatedWrites = supportsConsolidatedWrites;
        }
    }

    private static class TestingRestorableUndeterminedInputs
            extends TestingRestorable
    {
        @Override
        public Optional<Set<String>> getInputChannels()
        {
            return Optional.empty();
        }
    }
}
