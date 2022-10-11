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
package io.prestosql.plugin.hive.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

public class PrestoAvroSerDeTest
{
    private PrestoAvroSerDe prestoAvroSerDeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        prestoAvroSerDeUnderTest = new PrestoAvroSerDe();
    }

    @Test
    public void testDetermineSchemaOrReturnErrorSchema()
    {
        // Setup
        final Configuration conf = new Configuration(false);
        final Properties props = new Properties();

        // Run the test
        final Schema result = prestoAvroSerDeUnderTest.determineSchemaOrReturnErrorSchema(conf, props);
    }
}
