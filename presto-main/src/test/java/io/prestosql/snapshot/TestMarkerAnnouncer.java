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

import io.prestosql.spi.QueryId;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.split.SplitSource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.OptionalLong;
import java.util.function.Supplier;

import static io.prestosql.SessionTestUtils.TEST_SNAPSHOT_SESSION;
import static io.prestosql.testing.TestingSnapshotUtils.NOOP_SNAPSHOT_UTILS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMarkerAnnouncer
{
    private MarkerSplitSource markerSplitSource1;
    private MarkerSplitSource markerSplitSource2;
    private MarkerAnnouncer announcer;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        announcer = new MarkerAnnouncer(2);
        announcer.setSnapshotManager(new QuerySnapshotManager(new QueryId("query"), NOOP_SNAPSHOT_UTILS, TEST_SNAPSHOT_SESSION));
        markerSplitSource1 = announcer.createMarkerSplitSource(mock(SplitSource.class), new PlanNodeId("node1"));
        markerSplitSource2 = announcer.createMarkerSplitSource(mock(SplitSource.class), new PlanNodeId("node2"));
    }

    @DataProvider
    public Object[][] initializers()
    {
        return new Supplier[][] {
                {() -> 1L},
                {this::resumeSnapshot}
        };
    }

    @Test(dataProvider = "initializers")
    public void testShouldGenerateMarker(Supplier<Long> initializer)
    {
        long startSnapshotId = initializer.get();

        OptionalLong snapshotId = announcer.shouldGenerateMarker(markerSplitSource1);
        assertFalse(snapshotId.isPresent());
        announcer.incrementSplitCount(2);
        snapshotId = announcer.shouldGenerateMarker(markerSplitSource1);
        assertTrue(snapshotId.isPresent());
        assertEquals(snapshotId.getAsLong(), startSnapshotId);

        snapshotId = announcer.shouldGenerateMarker(markerSplitSource2);
        assertTrue(snapshotId.isPresent());
        assertEquals(snapshotId.getAsLong(), startSnapshotId);
    }

    @Test(dataProvider = "initializers")
    public void testForceGenerateMarker(Supplier<Long> initializer)
    {
        long startSnapshotId = initializer.get();

        announcer.incrementSplitCount(2);
        OptionalLong snapshotId = announcer.shouldGenerateMarker(markerSplitSource1);
        assertTrue(snapshotId.isPresent());
        assertEquals(snapshotId.getAsLong(), startSnapshotId);

        long sid = announcer.forceGenerateMarker(markerSplitSource2);
        assertEquals(sid, startSnapshotId);

        sid = announcer.forceGenerateMarker(markerSplitSource2);
        assertEquals(sid, startSnapshotId + 1);

        sid = announcer.forceGenerateMarker(markerSplitSource2);
        assertEquals(sid, startSnapshotId + 2);
    }

    @Test(dataProvider = "initializers")
    public void testDeactivateSplitSource(Supplier<Long> initializer)
    {
        long startSnapshotId = initializer.get();

        announcer.incrementSplitCount(2);
        announcer.shouldGenerateMarker(markerSplitSource1);

        announcer.incrementSplitCount(2);
        OptionalLong snapshotId = announcer.shouldGenerateMarker(markerSplitSource1);
        assertFalse(snapshotId.isPresent());

        announcer.deactivateSplitSource(markerSplitSource2);
        announcer.incrementSplitCount(2);
        snapshotId = announcer.shouldGenerateMarker(markerSplitSource1);
        assertTrue(snapshotId.isPresent());
        assertEquals(snapshotId.getAsLong(), startSnapshotId + 1);

        announcer.incrementSplitCount(2);
        snapshotId = announcer.shouldGenerateMarker(markerSplitSource1);
        assertTrue(snapshotId.isPresent());
        assertEquals(snapshotId.getAsLong(), startSnapshotId + 2);
    }

    private long resumeSnapshot()
    {
        announcer.deactivateSplitSource(markerSplitSource2);
        announcer.resumeSnapshot(5);
        return 2;
    }
}
