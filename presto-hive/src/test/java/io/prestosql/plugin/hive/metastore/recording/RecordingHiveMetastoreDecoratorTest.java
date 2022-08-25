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
package io.prestosql.plugin.hive.metastore.recording;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.mockito.MockitoAnnotations.initMocks;

public class RecordingHiveMetastoreDecoratorTest
{
    @Mock
    private HiveMetastoreRecording mockRecording;

    private RecordingHiveMetastoreDecorator recordingHiveMetastoreDecoratorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        recordingHiveMetastoreDecoratorUnderTest = new RecordingHiveMetastoreDecorator(mockRecording);
    }

    @Test
    public void testGetPriority()
    {
        recordingHiveMetastoreDecoratorUnderTest.getPriority();
    }

    @Test
    public void testDecorate() throws Exception
    {
        // Setup
        final HiveMetastore hiveMetastore = FileHiveMetastore.createTestingFileHiveMetastore(new File("file"));

        // Run the test
        final HiveMetastore result = recordingHiveMetastoreDecoratorUnderTest.decorate(hiveMetastore);

        // Verify the results
    }
}
