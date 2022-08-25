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
package io.prestosql.spi.heuristicindex;

import io.prestosql.spi.connector.CreateIndexMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class IndexRecordTest
{
    private IndexRecord indexRecordUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        indexRecordUnderTest = new IndexRecord("name", "user", "qualifiedTable", new String[]{"columns"}, "indexType",
                0L,
                Arrays.asList("value"), Arrays.asList("value"));
    }

    @Test
    public void testSerializeKey()
    {
        assertEquals("result", indexRecordUnderTest.serializeKey());
    }

    @Test
    public void testSerializeValue() throws Exception
    {
        assertEquals("result", indexRecordUnderTest.serializeValue());
    }

    @Test
    public void testIsInProgressRecord() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = indexRecordUnderTest.isInProgressRecord();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetProperties() throws Exception
    {
        // Setup
        final Properties expectedResult = new Properties();

        // Run the test
        final Properties result = indexRecordUnderTest.getProperties();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetProperty() throws Exception
    {
        assertEquals("result", indexRecordUnderTest.getProperty("key"));
    }

    @Test
    public void testGetLevel() throws Exception
    {
        assertEquals(CreateIndexMetadata.Level.STRIPE, indexRecordUnderTest.getLevel());
    }

    @Test
    public void testIsAutoloadEnabled() throws Exception
    {
        assertTrue(indexRecordUnderTest.isAutoloadEnabled());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(indexRecordUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, indexRecordUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", indexRecordUnderTest.toString());
    }
}
