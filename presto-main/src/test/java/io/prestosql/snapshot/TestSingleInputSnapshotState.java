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
import io.prestosql.execution.TaskId;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Test(singleThreaded = true)
public class TestSingleInputSnapshotState
{
    private static final Page regularPage = new Page(1);
    private static final MarkerPage marker1 = MarkerPage.snapshotPage(1);
    private static final MarkerPage marker2 = MarkerPage.snapshotPage(2);
    private static final MarkerPage resume1 = MarkerPage.resumePage(1);
    private static final MarkerPage resume2 = MarkerPage.resumePage(2);
    private static final SnapshotStateId snapshotId1 = createSnapshotStateId(1);
    private static final SnapshotStateId snapshotId2 = createSnapshotStateId(2);

    private static SnapshotStateId createSnapshotStateId(long snapshotId)
    {
        return new SnapshotStateId(snapshotId, new TaskId("query", 1, 1));
    }

    private TaskSnapshotManager snapshotManager;
    private TestingRestorable restorable;
    private SingleInputSnapshotState state;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        snapshotManager = mock(TaskSnapshotManager.class);
        restorable = new TestingRestorable();
        restorable.state = 100;
        state = new SingleInputSnapshotState(restorable, snapshotManager, null, TestSingleInputSnapshotState::createSnapshotStateId, TestSingleInputSnapshotState::createSnapshotStateId);
    }

    private boolean processPage(Page page)
    {
        boolean ret = state.processPage(page);
        restorable.state++;
        return ret;
    }

    @Test
    public void testStaticConstructor()
    {
        ScheduledExecutorService scheduler = newScheduledThreadPool(4);
        DriverContext driverContext = createTaskContext(scheduler, scheduler, TEST_SNAPSHOT_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("planNodeId"), "test");

        SingleInputSnapshotState state = SingleInputSnapshotState.forOperator(mock(Operator.class), operatorContext);
        state.processPage(regularPage);
    }

    @Test
    public void testRegularPage()
    {
        boolean ret = processPage(regularPage);
        Assert.assertFalse(ret);
        Assert.assertNull(state.nextMarker());
    }

    @Test
    public void testMarkerPage()
    {
        processPage(regularPage);

        boolean ret = processPage(marker1);
        Assert.assertTrue(ret);
        MarkerPage markerPage = state.nextMarker();
        Assert.assertEquals(markerPage.getSnapshotId(), 1);
        Assert.assertFalse(markerPage.isResuming());
        Assert.assertNull(state.nextMarker());
    }

    @Test
    public void test2MarkerPages()
    {
        processPage(regularPage);

        boolean ret = processPage(marker1);
        Assert.assertTrue(ret);

        processPage(regularPage);

        ret = state.processPage(marker2);
        Assert.assertTrue(ret);

        MarkerPage markerPage = state.nextMarker();
        Assert.assertEquals(markerPage.getSnapshotId(), 1);
        Assert.assertFalse(markerPage.isResuming());
        markerPage = state.nextMarker();
        Assert.assertEquals(markerPage.getSnapshotId(), 2);
        Assert.assertFalse(markerPage.isResuming());
        Assert.assertNull(state.nextMarker());
    }

    @Test
    public void testResumeMarker()
            throws Exception
    {
        processPage(regularPage);

        int saved = restorable.state;
        processPage(marker1);

        processPage(regularPage);

        when(snapshotManager.loadState(anyObject())).thenReturn(Optional.of(saved));
        boolean ret = state.processPage(resume1);
        Assert.assertTrue(ret);
        Assert.assertEquals(restorable.state, saved);

        MarkerPage markerPage = state.nextMarker();
        Assert.assertEquals(markerPage.getSnapshotId(), 1);
        Assert.assertTrue(markerPage.isResuming());
        Assert.assertNull(state.nextMarker());
    }

    @Test
    public void test2Resumes()
            throws Exception
    {
        processPage(regularPage);

        int saved1 = restorable.state;
        processPage(marker1);
        state.nextMarker();

        processPage(regularPage);

        int saved2 = restorable.state;
        processPage(marker2);
        state.nextMarker();

        processPage(regularPage);

        when(snapshotManager.loadState(snapshotId2)).thenReturn(Optional.of(saved2));
        state.processPage(resume2);

        processPage(regularPage);

        when(snapshotManager.loadState(snapshotId1)).thenReturn(Optional.of(saved1));
        state.processPage(resume1);
        Assert.assertEquals(restorable.state, saved1);

        state.processPage(regularPage);

        MarkerPage markerPage = state.nextMarker();
        Assert.assertEquals(markerPage.getSnapshotId(), 1);
        Assert.assertTrue(markerPage.isResuming());
        Assert.assertNull(state.nextMarker());
    }

    @Test
    public void testResumeBacktrack()
            throws Exception
    {
        SingleInputSnapshotState state = new SingleInputSnapshotState(restorable, snapshotManager, null, TestSingleInputSnapshotState::createSnapshotStateId, TestSingleInputSnapshotState::createSnapshotStateId);
        state.processPage(regularPage);
        restorable.state++;
        int saved1 = restorable.state;
        state.processPage(marker1);
        restorable.state++;
        state.processPage(regularPage);
        restorable.state++;

        when(snapshotManager.loadState(createSnapshotStateId(marker2.getSnapshotId()))).thenReturn(Optional.of(saved1));
        state.processPage(resume2);
        Assert.assertEquals(restorable.state, saved1);
    }

    @Test
    public void testSaveLoadSpilledFiles()
            throws Exception
    {
        SingleInputSnapshotState state = new SingleInputSnapshotState(
                new TestingSpillableRestorable(),
                snapshotManager,
                null,
                TestSingleInputSnapshotState::createSnapshotStateId,
                TestSingleInputSnapshotState::createSnapshotStateId);
        state.processPage(marker1);
        when(snapshotManager.loadState(anyObject())).thenReturn(Optional.of(1));
        when(snapshotManager.loadFile(anyObject(), anyObject()))
                .thenReturn(Boolean.TRUE) // 2 calls for each resume attempt for 2 files
                .thenReturn(Boolean.TRUE)
                .thenReturn(Boolean.FALSE) // 1 call for the failure; 2nd call won't happen
                .thenReturn(null); // 1 call for the failure; 2nd call won't happen
        state.processPage(resume1);
        state.processPage(resume1);
        state.processPage(resume1);
        verify(snapshotManager, times(2)).storeFile(anyObject(), anyObject());
        verify(snapshotManager, times(4)).loadFile(anyObject(), anyObject());
        verify(snapshotManager, times(1)).succeededToRestore(anyObject());
        verify(snapshotManager, times(2)).failedToRestore(anyObject(), anyBoolean());
    }

    @Test
    public void testStoreLoadConsolidatedSnapshot()
            throws Exception
    {
        TestingRestorable restorable = new TestingRestorable();
        restorable.setSupportsConsolidatedWrites(true);
        SingleInputSnapshotState state = new SingleInputSnapshotState(
                restorable,
                snapshotManager,
                null,
                TestSingleInputSnapshotState::createSnapshotStateId,
                TestSingleInputSnapshotState::createSnapshotStateId);
        state.processPage(marker1);
        when(snapshotManager.loadConsolidatedState(anyObject())).thenReturn(Optional.of(0));
        state.processPage(resume1);
        verify(snapshotManager, times(0)).storeState(anyObject(), anyObject());
        verify(snapshotManager, times(1)).storeConsolidatedState(anyObject(), anyObject());
        verify(snapshotManager, times(0)).loadState(anyObject());
        verify(snapshotManager, times(1)).loadConsolidatedState(anyObject());
    }

    @Test
    public void testStoreLoadComplexSnapshot()
            throws Exception
    {
        TestingRestorable restorable = new TestingRestorable();
        restorable.setSupportsConsolidatedWrites(false);
        SingleInputSnapshotState state = new SingleInputSnapshotState(
                restorable,
                snapshotManager,
                null,
                TestSingleInputSnapshotState::createSnapshotStateId,
                TestSingleInputSnapshotState::createSnapshotStateId);
        state.processPage(marker1);
        when(snapshotManager.loadState(anyObject())).thenReturn(Optional.of(0));
        state.processPage(resume1);
        verify(snapshotManager, times(0)).storeConsolidatedState(anyObject(), anyObject());
        verify(snapshotManager, times(1)).storeState(anyObject(), anyObject());
        verify(snapshotManager, times(0)).loadConsolidatedState(anyObject());
        verify(snapshotManager, times(1)).loadState(anyObject());
    }

    @RestorableConfig(uncapturedFields = {"supportsConsolidatedWrites"})
    private static class TestingRestorable
            implements Restorable
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
        public boolean supportsConsolidatedWrites()
        {
            return this.supportsConsolidatedWrites;
        }

        public void setSupportsConsolidatedWrites(boolean supportsConsolidatedWrites)
        {
            this.supportsConsolidatedWrites = supportsConsolidatedWrites;
        }
    }

    private static class TestingSpillableRestorable
            extends TestingRestorable
            implements Spillable
    {
        @Override
        public boolean isSpilled()
        {
            return true;
        }

        @Override
        public List<Path> getSpilledFilePaths()
        {
            return ImmutableList.of(Paths.get("path1"), Paths.get("path2"));
        }
    }
}
