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
import com.google.common.util.concurrent.Futures;
import io.prestosql.execution.Lifespan;
import io.prestosql.metadata.Split;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.SplitSource;
import io.prestosql.split.SplitSource.SplitBatch;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMarkerSplitSource
{
    private static final QueryId queryId = QueryId.valueOf("test_query");
    private static final CatalogName catalogName = new CatalogName("test_catalog");
    private static final Lifespan lifespan = Lifespan.taskWide();
    private static final Split split1 = new Split(catalogName, new EmptySplit(catalogName), lifespan);
    private static final Split split2 = new Split(catalogName, new EmptySplit(catalogName), lifespan);
    private static final Split split3 = new Split(catalogName, new EmptySplit(catalogName), lifespan);
    private static final List<Split> splits = Arrays.asList(split1, split2, split3);
    private static final SplitBatch testBatch = new SplitBatch(splits, false);
    private static final SplitBatch lastBatch = new SplitBatch(splits, true);
    private static final SplitBatch testBatch1 = new SplitBatch(Collections.singletonList(split1), false);
    private static final SplitBatch testBatch2 = new SplitBatch(Collections.singletonList(split2), false);
    private static final SplitBatch emptyLastBatch = new SplitBatch(ImmutableList.of(), true);

    private SplitSource splitSource;
    private MarkerAnnouncer announcer;
    private MarkerSplitSource markerSource;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        splitSource = mock(SplitSource.class);
        announcer = mock(MarkerAnnouncer.class);
        markerSource = new MarkerSplitSource(splitSource, announcer);

        when(splitSource.getCatalogName()).thenReturn(catalogName);
    }

    @DataProvider
    public Object[][] initializers()
    {
        return new Runnable[][] {
                {() -> {}},
                {this::resumeSnapshot},
                {this::resumeSnapshotBeforeCaptured},
                {this::resumeSnapshotToFirst},
                {this::resumeSnapshotToSecond}
        };
    }

    @Test(dataProvider = "initializers")
    public void testGetNextBatchNoSnapshot(Runnable initializer)
            throws Exception
    {
        initializer.run();

        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(testBatch));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        assertEquals(result.get(), testBatch);
    }

    @Test(dataProvider = "initializers")
    public void testGetNextBatchSnapshotBefore(Runnable initializer)
            throws Exception
    {
        initializer.run();

        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(10));

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        SplitBatch batch = result.get();
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
    }

    @Test(dataProvider = "initializers")
    public void testGetNextBatchSnapshotAfter(Runnable initializer)
            throws Exception
    {
        initializer.run();

        when(announcer.shouldGenerateMarker(anyObject()))
                .thenReturn(OptionalLong.empty())
                .thenReturn(OptionalLong.of(10));
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(testBatch));

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        SplitBatch batch = result.get();
        assertEquals(batch.getSplits().size(), testBatch.getSplits().size());
        assertEquals(batch.getSplits(), testBatch.getSplits());
        batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
    }

    @Test(dataProvider = "initializers")
    public void testGetNextBatchLastBatchSnapshot(Runnable initializer)
            throws Exception
    {
        initializer.run();

        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(lastBatch));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(announcer.forceGenerateMarker(anyObject())).thenReturn(10L);

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        SplitBatch batch = result.get();
        assertEquals(batch.getSplits().size(), testBatch.getSplits().size());
        assertEquals(batch.getSplits(), testBatch.getSplits());
        batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
    }

    private Future<SplitBatch> getNextBatchIgnoreMarkerZero(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        Future<SplitBatch> result = markerSource.getNextBatch(partitionHandle, lifespan, maxSize);
        while (true) {
            try {
                if (!containsMarkerZero(result.get())) {
                    break;
                }
            }
            catch (Exception e) {
                // do nothing.
            }
            result = markerSource.getNextBatch(partitionHandle, lifespan, maxSize);
        }
        return result;
    }

    private boolean containsMarkerZero(SplitBatch splitBatch)
    {
        if (splitBatch == null) {
            return false;
        }
        for (Split split : splitBatch.getSplits()) {
            if (split.getConnectorSplit() instanceof MarkerSplit && ((MarkerSplit) split.getConnectorSplit()).getSnapshotId() == 0) {
                return true;
            }
        }
        return false;
    }

    private void resumeSnapshot()
    {
        markerSource.resumeSnapshot(5);
        // Ignore resume marker
        getNextBatchIgnoreMarkerZero(null, lifespan, 2);
    }

    private void resumeSnapshotBeforeCaptured()
    {
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(5));
        getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        markerSource.resumeSnapshot(4);
        // Ignore resume marker
        getNextBatchIgnoreMarkerZero(null, lifespan, 2);
    }

    private void resumeSnapshotToFirst()
    {
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(5));
        getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        markerSource.resumeSnapshot(5);
        // Ignore resume marker
        getNextBatchIgnoreMarkerZero(null, lifespan, 2);
    }

    private void resumeSnapshotToSecond()
    {
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt()))
                .thenReturn(Futures.immediateFuture(testBatch1))
                .thenReturn(Futures.immediateFuture(testBatch2));
        when(announcer.shouldGenerateMarker(anyObject()))
                .thenReturn(OptionalLong.of(5))
                .thenReturn(OptionalLong.of(6));
        getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        markerSource.resumeSnapshot(6);
        // Ignore resume marker
        getNextBatchIgnoreMarkerZero(null, lifespan, 3);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testResumeSnapshotToLargeId()
    {
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(5));
        getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        markerSource.resumeSnapshot(7);
        // Ignore resume marker
        getNextBatchIgnoreMarkerZero(null, lifespan, 2);
    }

    @Test
    public void testResumeFromBeginning()
            throws Exception
    {
        Future<SplitBatch> result = markerSource.getNextBatch(null, lifespan, 3);
        assertTrue(containsMarkerZero(result.get()));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(5));
        result = markerSource.getNextBatch(null, lifespan, 3);
        assertFalse(containsMarkerZero(result.get()));
        markerSource.resumeSnapshot(3);
        result = markerSource.getNextBatch(null, lifespan, 3);
        assertFalse(containsMarkerZero(result.get()));
        markerSource.resumeSnapshot(0);
        result = markerSource.getNextBatch(null, lifespan, 3);
        assertTrue(containsMarkerZero(result.get()));
    }

    private void getAndResume(boolean exhaust)
            throws Exception
    {
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt()))
                .thenReturn(Futures.immediateFuture(exhaust ? lastBatch : testBatch));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(announcer.forceGenerateMarker(anyObject())).thenReturn(5L);
        Future<SplitBatch> batch = getNextBatchIgnoreMarkerZero(null, lifespan, 4);
        if (exhaust) {
            assertFalse(batch.get().isLastBatch());
            assertEquals(batch.get().getSplits().size(), lastBatch.getSplits().size());
            batch = getNextBatchIgnoreMarkerZero(null, lifespan, 4);
            assertTrue(batch.get().isLastBatch());
            assertEquals(batch.get().getSplits().size(), 1);
        }
        else {
            assertFalse(batch.get().isLastBatch());
            assertEquals(batch.get().getSplits().size(), lastBatch.getSplits().size());
        }

        markerSource.resumeSnapshot(3);
        SplitBatch result = getNextBatchIgnoreMarkerZero(null, lifespan, 2).get();
        assertFalse(result.isLastBatch());
        List<Split> splits = result.getSplits();
        assertEquals(splits.size(), 1);
        ConnectorSplit split = splits.get(0).getConnectorSplit();
        assertTrue(split instanceof MarkerSplit);
        MarkerSplit marker = (MarkerSplit) split;
        assertEquals(marker.getSnapshotId(), 3);
        assertTrue(marker.isResuming());
    }

    @Test
    public void testGetNextBatchAfterResume()
            throws Exception
    {
        getAndResume(false);
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt()))
                .thenReturn(Futures.immediateFuture(testBatch1))
                .thenReturn(Futures.immediateFuture(null));

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        List<Split> splits = result.get().getSplits();
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0), split1);
        assertEquals(splits.get(1), split2);
        assertFalse(markerSource.isFinished());

        result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        splits = result.get().getSplits();
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0), split3);
        assertEquals(splits.get(1), split1);

        result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        assertNull(result.get());

        // Once in getAndResume and twice above
        verify(splitSource, times(3)).getNextBatch(anyObject(), anyObject(), anyInt());
    }

    @Test
    public void testGetNextBatchAfterExhaust()
            throws Exception
    {
        getAndResume(true);
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt()))
                .thenThrow(new IllegalStateException());

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        List<Split> splits = result.get().getSplits();
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0), split1);
        assertEquals(splits.get(1), split2);
        assertFalse(markerSource.isFinished());

        result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        splits = result.get().getSplits();
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0), split3);
        result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        splits = result.get().getSplits();
        assertEquals(splits.size(), 1);
        assertTrue(splits.get(0).getConnectorSplit() instanceof MarkerSplit);
        assertTrue(markerSource.isFinished());
    }

    @Test
    public void testComplexResume()
            throws Exception
    {
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt()))
                .thenReturn(Futures.immediateFuture(testBatch))
                .thenReturn(Futures.immediateFuture(null));

        // 1, 2, 3: not involved
        // 4, 5, 6: snapshot; first = 4
        // 7, 8, 9: not involved
        // resume to 2; first = empty
        // 10     : not involved
        // 11, 12 : snapshot; first = 11
        // resume to 2; first = empty
        // 13, 14 : not involved
        // 15, 16 : snapshot; first = 15
        // resume to 13; position = 0

        when(announcer.shouldGenerateMarker(anyObject()))
                .thenReturn(OptionalLong.empty()) // Trigger reading of 3 splits
                .thenReturn(OptionalLong.of(4))
                .thenReturn(OptionalLong.of(5))
                .thenReturn(OptionalLong.of(6))
                .thenReturn(OptionalLong.empty()) // Trigger reading of 3 splits
                .thenReturn(OptionalLong.of(11))
                .thenReturn(OptionalLong.of(12))
                .thenReturn(OptionalLong.empty()) // Trigger reading of 3 splits
                .thenReturn(OptionalLong.of(15))
                .thenReturn(OptionalLong.of(16))
                .thenReturn(OptionalLong.empty());

        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Read 3 splits
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Snapshot 4
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // 5
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // 6
        markerSource.resumeSnapshot(2);
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Resume 2

        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Read 3 splits
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // 11
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // 12
        markerSource.resumeSnapshot(2);
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Resume 2

        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Read 3 splits
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // 15
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // 16
        markerSource.resumeSnapshot(13);
        getNextBatchIgnoreMarkerZero(null, lifespan, 3); // Resume 13

        SplitBatch batch = getNextBatchIgnoreMarkerZero(null, lifespan, 4).get();
        assertEquals(batch.getSplits().size(), 3);
    }

    @Test
    public void testMarkerDependency()
            throws Exception
    {
        MarkerSplitSource otherSource = mock(MarkerSplitSource.class);
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(testBatch1));
        markerSource.addDependency(otherSource);

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        List<Split> splits = result.get().getSplits();
        assertTrue(splits.isEmpty());
        markerSource.finishDependency(otherSource);
        result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        splits = result.get().getSplits();
        assertTrue(!splits.isEmpty());

        markerSource.resumeSnapshot(0);
        result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        splits = result.get().getSplits();
        assertTrue(splits.isEmpty());
        markerSource.finishDependency(otherSource);
        result = getNextBatchIgnoreMarkerZero(null, lifespan, 2);
        splits = result.get().getSplits();
        assertTrue(!splits.isEmpty());
    }

    @Test(dataProvider = "initializers")
    public void testGetNextBatchWithFinish(Runnable initializer)
            throws Exception
    {
        initializer.run();

        final boolean[] closed = new boolean[1];
        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt()))
                .thenReturn(Futures.immediateFuture(testBatch))
                .then(invocation -> {
                    if (closed[0]) {
                        throw new IllegalStateException();
                    }
                    return Futures.immediateFuture(emptyLastBatch);
                });
        when(splitSource.isFinished())
                .thenReturn(true)
                .then(invocation -> {
                    if (closed[0]) {
                        throw new IllegalStateException();
                    }
                    return true;
                });
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());

        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        assertEquals(result.get(), testBatch);
        if (markerSource.isFinished()) {
            closed[0] = true;
            markerSource.close();
        }
        markerSource.resumeSnapshot(0);
        result = getNextBatchIgnoreMarkerZero(null, lifespan, 10);
        assertEquals(result.get().getSplits(), testBatch.getSplits());
    }

    @Test
    public void test2Resumes()
            throws Exception
    {
        markerSource.resumeSnapshot(7);
        // Should clear earlier resume snapshot id
        markerSource.resumeSnapshot(0);

        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(3));
        Future<SplitBatch> result = getNextBatchIgnoreMarkerZero(null, lifespan, 3);
        assertTrue(result.isDone());
        List<Split> splits = result.get().getSplits();
        assertEquals(splits.size(), 1);
        assertTrue(splits.get(0).getConnectorSplit() instanceof MarkerSplit);
        // Confirm what's returned is the snapshot marker (as opposed to the resume marker for snapshot id 7)
        assertFalse(((MarkerSplit) splits.get(0).getConnectorSplit()).isResuming());
    }

    @Test
    public void testUnionSourcesEarly()
            throws ExecutionException, InterruptedException
    {
        MarkerSplitSource other = mock(MarkerSplitSource.class);
        markerSource.addUnionSources(ImmutableList.of(markerSource, other));

        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(lastBatch));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(announcer.forceGenerateMarker(anyObject())).thenReturn(1L);

        SplitBatch batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertEquals(batch.getSplits().size(), 3);
        batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertFalse(batch.isLastBatch());
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
        assertEquals(((MarkerSplit) batch.getSplits().get(0).getConnectorSplit()).getSnapshotId(), 1);
        verify(other).finishUnionSource(markerSource, OptionalLong.empty());

        batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertFalse(batch.isLastBatch());
        assertEquals(batch.getSplits().size(), 0);

        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(2));
        batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertFalse(batch.isLastBatch());
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
        assertEquals(((MarkerSplit) batch.getSplits().get(0).getConnectorSplit()).getSnapshotId(), 2);

        markerSource.finishUnionSource(other, OptionalLong.of(3));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(3));
        batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertTrue(batch.isLastBatch());
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
        assertEquals(((MarkerSplit) batch.getSplits().get(0).getConnectorSplit()).getSnapshotId(), 3);
    }

    @Test
    public void testUnionSourcesSame()
            throws ExecutionException, InterruptedException
    {
        MarkerSplitSource other = mock(MarkerSplitSource.class);
        markerSource.addUnionSources(ImmutableList.of(markerSource, other));

        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(lastBatch));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(announcer.forceGenerateMarker(anyObject())).thenReturn(1L);

        getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();

        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(2));
        getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();

        markerSource.finishUnionSource(other, OptionalLong.of(2));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.of(3));
        SplitBatch batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertTrue(batch.isLastBatch());
        assertEquals(batch.getSplits().size(), 0);
    }

    @Test
    public void testUnionSourcesLate()
            throws ExecutionException, InterruptedException
    {
        MarkerSplitSource other = mock(MarkerSplitSource.class);
        markerSource.addUnionSources(ImmutableList.of(markerSource, other));

        when(splitSource.getNextBatch(anyObject(), anyObject(), anyInt())).thenReturn(Futures.immediateFuture(lastBatch));
        when(announcer.shouldGenerateMarker(anyObject())).thenReturn(OptionalLong.empty());
        when(announcer.forceGenerateMarker(anyObject())).thenReturn(1L);

        getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();

        markerSource.finishUnionSource(other, OptionalLong.empty());
        SplitBatch batch = getNextBatchIgnoreMarkerZero(null, lifespan, 3).get();
        assertTrue(batch.isLastBatch());
        assertEquals(batch.getSplits().size(), 1);
        assertTrue(batch.getSplits().get(0).getConnectorSplit() instanceof MarkerSplit);
        assertEquals(((MarkerSplit) batch.getSplits().get(0).getConnectorSplit()).getSnapshotId(), 1);
        verify(other).finishUnionSource(markerSource, OptionalLong.of(1));
    }
}
