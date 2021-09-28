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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.testing.TestingPagesSerdeFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.UpdateMemory.NOOP;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestMarkDistinctHash
{
    @Test
    public void testMarkDistinctHashSnapshot()
    {
        MarkDistinctHash markDistinctHash = new MarkDistinctHash(TEST_SESSION,
                ImmutableList.of(BIGINT),
                new int[1],
                Optional.of(1),
                new JoinCompiler(createTestMetadataManager()), NOOP);

        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(10, 0)
                .addSequencePage(10, 10)
                .build();

        PagesSerde serde = TestingPagesSerdeFactory.testingPagesSerde();

        markDistinctHash.markDistinctRows(input.get(0)).process();

        Object snapshot = markDistinctHash.capture(serde);
        assertEquals(SnapshotTestUtil.toSimpleSnapshotMapping(snapshot), createExpectedMapping());

        markDistinctHash.markDistinctRows(input.get(1)).process();
        Object snapshotWithoutRestore = markDistinctHash.capture(serde);
        markDistinctHash.restore(snapshot, serde);
        markDistinctHash.markDistinctRows(input.get(1)).process();
        Object snapshotWithRestore = markDistinctHash.capture(serde);
        assertEquals(SnapshotTestUtil.toFullSnapshotMapping(snapshotWithRestore), SnapshotTestUtil.toFullSnapshotMapping(snapshotWithoutRestore));
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("nextDistinctId", 10L);
        return expectedMapping;
    }
}
