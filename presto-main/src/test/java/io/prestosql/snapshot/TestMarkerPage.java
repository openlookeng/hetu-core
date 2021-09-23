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

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import static io.prestosql.testing.TestingPagesSerdeFactory.TESTING_SERDE_FACTORY;

public class TestMarkerPage
{
    @Test
    public void testSerde()
    {
        MarkerPage page = new MarkerPage(100, true);
        SerializedPage spage = SerializedPage.forMarker(page);
        page = spage.toMarker();
        Assert.assertEquals(page.getSnapshotId(), 100);
        Assert.assertTrue(page.isResuming());
    }

    @Test
    public void testStaticSerde()
    {
        PagesSerde serde = TESTING_SERDE_FACTORY.createPagesSerde();

        MarkerPage marker1 = MarkerPage.snapshotPage(1);
        SerializedPage serializedPage = serde.serialize(marker1);
        Page deserializedPage = serde.deserialize(serializedPage);
        Assert.assertTrue(deserializedPage instanceof MarkerPage);
        marker1 = (MarkerPage) deserializedPage;
        Assert.assertEquals(marker1.getSnapshotId(), 1);
        Assert.assertFalse(marker1.isResuming());

        MarkerPage resume1 = MarkerPage.resumePage(1);
        serializedPage = serde.serialize(resume1);
        deserializedPage = serde.deserialize(serializedPage);
        Assert.assertTrue(deserializedPage instanceof MarkerPage);
        resume1 = (MarkerPage) deserializedPage;
        Assert.assertEquals(resume1.getSnapshotId(), 1);
        Assert.assertTrue(resume1.isResuming());

        Page regular = new Page(1);
        serializedPage = serde.serialize(regular);
        deserializedPage = serde.deserialize(serializedPage);
        Assert.assertFalse(deserializedPage instanceof MarkerPage);
    }
}
