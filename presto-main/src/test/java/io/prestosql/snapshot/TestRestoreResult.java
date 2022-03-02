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

import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

public class TestRestoreResult
{
    /**
     * Test getters, setters, equals and hashcode
     */
    @Test
    public void testBasics()
    {
        // Test getters and setters
        long snapshotId = 1;
        RestoreResult restoreResult = new RestoreResult();
        restoreResult.setSnapshotResult(snapshotId, SnapshotResult.IN_PROGRESS);
        Assert.assertEquals(restoreResult.getSnapshotId(), snapshotId);
        Assert.assertEquals(restoreResult.getSnapshotInfo().getSnapshotResult(), SnapshotResult.IN_PROGRESS);
        restoreResult.setSnapshotResult(snapshotId, SnapshotResult.SUCCESSFUL);
        Assert.assertEquals(restoreResult.getSnapshotInfo().getSnapshotResult(), SnapshotResult.SUCCESSFUL);

        // Test equals
        RestoreResult restoreResult2 = new RestoreResult(snapshotId, SnapshotInfo.withStatus(SnapshotResult.SUCCESSFUL));
        RestoreResult restoreResult3 = new RestoreResult(snapshotId, SnapshotInfo.withStatus(SnapshotResult.FAILED));
        Assert.assertTrue(restoreResult.equals(restoreResult2));
        Assert.assertFalse(restoreResult.equals(restoreResult3));

        // Test hashcode
        Assert.assertEquals(restoreResult.hashCode(), restoreResult2.hashCode());
        Assert.assertEquals(restoreResult.hashCode(), restoreResult3.hashCode());
    }
}
