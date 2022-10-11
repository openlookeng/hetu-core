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
package io.prestosql.plugin.hive;

import io.prestosql.orc.OrcDataSink;
import io.prestosql.spi.Page;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class SnapshotTempFileWriterTest
{
    @Mock
    private OrcDataSink mockDataSink;

    private SnapshotTempFileWriter snapshotTempFileWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        snapshotTempFileWriterUnderTest = new SnapshotTempFileWriter(mockDataSink, Arrays.asList());
    }

    @Test
    public void testGetWrittenBytes()
    {
        // Setup
        // Run the test
        final long result = snapshotTempFileWriterUnderTest.getWrittenBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        // Run the test
        final long result = snapshotTempFileWriterUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testAppendRows()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        snapshotTempFileWriterUnderTest.appendRows(page);

        // Verify the results
    }

    @Test
    public void testCommit() throws Exception
    {
        // Setup
        // Run the test
        snapshotTempFileWriterUnderTest.commit();

        // Verify the results
    }

    @Test
    public void testRollback()
    {
        // Setup
        // Run the test
        snapshotTempFileWriterUnderTest.rollback();

        // Verify the results
    }

    @Test
    public void testGetValidationCpuNanos()
    {
        snapshotTempFileWriterUnderTest.getValidationCpuNanos();
    }

    @Test
    public void testToString() throws Exception
    {
        snapshotTempFileWriterUnderTest.toString();
    }
}
