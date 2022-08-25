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
package io.hetu.core.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class GlueIcebergUtilTest
{
    @Test
    public void testGetTableInput()
    {
        // Setup
        final Map<String, String> parameters = new HashMap<>();
        final TableInput expectedResult = new TableInput();
        expectedResult.setName("name");
        expectedResult.setDescription("description");
        expectedResult.setOwner("owner");
        expectedResult.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        expectedResult.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        expectedResult.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        column.setType("type");
        column.setComment("comment");
        storageDescriptor.setColumns(Arrays.asList(column));
        storageDescriptor.setLocation("location");
        storageDescriptor.setInputFormat("inputFormat");
        storageDescriptor.setOutputFormat("outputFormat");
        storageDescriptor.setCompressed(false);
        expectedResult.setStorageDescriptor(storageDescriptor);

        // Run the test
        final TableInput result = GlueIcebergUtil.getTableInput("tableName", Optional.of("value"), parameters);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetViewTableInput()
    {
        // Setup
        final Map<String, String> parameters = new HashMap<>();
        final TableInput expectedResult = new TableInput();
        expectedResult.setName("name");
        expectedResult.setDescription("description");
        expectedResult.setOwner("owner");
        expectedResult.setLastAccessTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        expectedResult.setLastAnalyzedTime(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime());
        expectedResult.setRetention(0);
        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        final Column column = new Column();
        column.setName("name");
        column.setType("type");
        column.setComment("comment");
        storageDescriptor.setColumns(Arrays.asList(column));
        storageDescriptor.setLocation("location");
        storageDescriptor.setInputFormat("inputFormat");
        storageDescriptor.setOutputFormat("outputFormat");
        storageDescriptor.setCompressed(false);
        expectedResult.setStorageDescriptor(storageDescriptor);

        // Run the test
        final TableInput result = GlueIcebergUtil.getViewTableInput("viewName", "viewOriginalText", "owner",
                parameters);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
