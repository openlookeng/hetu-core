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
package io.hetu.core.plugin.iceberg;

import org.apache.iceberg.FileContent;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class CommitTaskDataTest
{
    @Mock
    private MetricsWrapper mockMetrics;

    private CommitTaskData commitTaskDataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        commitTaskDataUnderTest = new CommitTaskData(
                "path",
                IcebergFileFormat.ORC,
                1,
                new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()),
                "partitionSpecJson",
                Optional.of("value"),
                FileContent.DATA,
                Optional.of("value"),
                Optional.of(Long.valueOf(1)),
                Optional.of(Long.valueOf(1)));
        commitTaskDataUnderTest = new CommitTaskData("path", IcebergFileFormat.ORC, 0L, mockMetrics,
                "partitionSpecJson",
                Optional.of("value"), FileContent.DATA, Optional.of("value"), Optional.of(0L),
                Optional.of(0L));
    }

    @Test
    public void test()
    {
        commitTaskDataUnderTest.getPath();
        commitTaskDataUnderTest.getFileFormat();
        commitTaskDataUnderTest.getFileSizeInBytes();
        commitTaskDataUnderTest.getMetrics();
        commitTaskDataUnderTest.getDeletedRowCount();
        commitTaskDataUnderTest.getPartitionSpecJson();
        commitTaskDataUnderTest.getPartitionDataJson();
        commitTaskDataUnderTest.getContent();
        commitTaskDataUnderTest.getReferencedDataFile();
        commitTaskDataUnderTest.getFileRecordCount();
    }
}
