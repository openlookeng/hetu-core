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
package io.prestosql.plugin.hive;

import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import io.prestosql.plugin.hive.s3.PrestoS3ClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.MockitoAnnotations.initMocks;

public class S3SelectCsvRecordReaderTest
{
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private HiveConfig mockHiveConfig;
    @Mock
    private Path mockPath;
    @Mock
    private PrestoS3ClientFactory mockS3ClientFactory;

    private S3SelectCsvRecordReader s3SelectCsvRecordReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        s3SelectCsvRecordReaderUnderTest = new S3SelectCsvRecordReader(new Configuration(), new HiveConfig(), new Path("pathstring", "parent"), 0L,
                0L, new Properties(), "ionSqlQuery", new PrestoS3ClientFactory());
    }

    @Test
    public void testBuildSelectObjectRequest()
    {
        // Setup
        final Properties schema = new Properties();
        final Path path = new Path("scheme", "authority", "path");

        // Run the test
        final SelectObjectContentRequest result = s3SelectCsvRecordReaderUnderTest.buildSelectObjectRequest(schema,
                "ionSqlQuery", path);

        // Verify the results
    }
}
