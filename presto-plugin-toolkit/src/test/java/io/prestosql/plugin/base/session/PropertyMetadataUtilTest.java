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
package io.prestosql.plugin.base.session;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.session.PropertyMetadata;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;

public class PropertyMetadataUtilTest
{
    @Test
    public void testDataSizeProperty1()
    {
        // Setup
        final DataSize defaultValue = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final PropertyMetadata<DataSize> result = PropertyMetadataUtil.dataSizeProperty("name", "description",
                defaultValue, false);

        // Verify the results
    }

    @Test
    public void testDataSizeProperty2()
    {
        // Setup
        final DataSize defaultValue = new DataSize(0.0, DataSize.Unit.BYTE);
        final Consumer<DataSize> mockValidation = mock(Consumer.class);

        // Run the test
        final PropertyMetadata<DataSize> result = PropertyMetadataUtil.dataSizeProperty("name", "description",
                defaultValue, mockValidation, false);

        // Verify the results
    }

    @Test
    public void testDurationProperty1()
    {
        // Setup
        final Duration defaultValue = new Duration(0.0, TimeUnit.MILLISECONDS);

        // Run the test
        final PropertyMetadata<Duration> result = PropertyMetadataUtil.durationProperty("name", "description",
                defaultValue, false);

        // Verify the results
    }

    @Test
    public void testDurationProperty2()
    {
        // Setup
        final Duration defaultValue = new Duration(0.0, TimeUnit.MILLISECONDS);
        final Consumer<Duration> mockValidation = mock(Consumer.class);

        // Run the test
        final PropertyMetadata<Duration> result = PropertyMetadataUtil.durationProperty("name", "description",
                defaultValue, mockValidation, false);

        // Verify the results
    }
}
