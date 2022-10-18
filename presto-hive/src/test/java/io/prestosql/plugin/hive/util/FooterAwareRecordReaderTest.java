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
package io.prestosql.plugin.hive.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class FooterAwareRecordReaderTest<K extends WritableComparable<?>, V extends Writable>
{
    @Mock
    private RecordReader<K, V> mockDelegate;
    @Mock
    private JobConf mockJob;

    private FooterAwareRecordReader<K, V> footerAwareRecordReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        footerAwareRecordReaderUnderTest = new FooterAwareRecordReader<>(mockDelegate, 10, mockJob);
    }

    @Test
    public void testNext() throws Exception
    {
        // Setup
        final K key = null;
        final V value = null;

        // Run the test
        final boolean result = footerAwareRecordReaderUnderTest.next(key, value);
    }

    @Test
    public void testCreateKey()
    {
        // Setup
        when(mockDelegate.createKey()).thenReturn(null);

        // Run the test
        final K result = footerAwareRecordReaderUnderTest.createKey();

        // Verify the results
    }

    @Test
    public void testCreateValue() throws Exception
    {
        // Setup
        when(mockDelegate.createValue()).thenReturn(null);

        // Run the test
        final V result = footerAwareRecordReaderUnderTest.createValue();

        // Verify the results
    }

    @Test
    public void testGetPos() throws Exception
    {
        // Setup
        when(mockDelegate.getPos()).thenReturn(0L);

        // Run the test
        final long result = footerAwareRecordReaderUnderTest.getPos();
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        footerAwareRecordReaderUnderTest.close();
    }

    @Test
    public void testGetProgress() throws Exception
    {
        // Setup
        when(mockDelegate.getProgress()).thenReturn(0.0f);

        // Run the test
        final float result = footerAwareRecordReaderUnderTest.getProgress();
    }
}
