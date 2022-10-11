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
package io.prestosql.plugin.hive.orc;

import io.airlift.units.DataSize;
import io.prestosql.orc.OrcWriteValidation;
import io.prestosql.orc.OrcWriterOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OrcWriterConfigTest
{
    private OrcWriterConfig orcWriterConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        orcWriterConfigUnderTest = new OrcWriterConfig();
    }

    @Test
    public void testToOrcWriterOptions()
    {
        // Setup
        // Run the test
        final OrcWriterOptions result = orcWriterConfigUnderTest.toOrcWriterOptions();

        // Verify the results
    }

    @Test
    public void testGetStripeMinSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcWriterConfigUnderTest.getStripeMinSize();
    }

    @Test
    public void testSetStripeMinSize()
    {
        // Setup
        final DataSize stripeMinSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setStripeMinSize(stripeMinSize);

        // Verify the results
    }

    @Test
    public void testGetStripeMaxSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcWriterConfigUnderTest.getStripeMaxSize();
    }

    @Test
    public void testSetStripeMaxSize()
    {
        // Setup
        final DataSize stripeMaxSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setStripeMaxSize(stripeMaxSize);

        // Verify the results
    }

    @Test
    public void testGetStripeMaxRowCount()
    {
        // Setup
        // Run the test
        final int result = orcWriterConfigUnderTest.getStripeMaxRowCount();
    }

    @Test
    public void testSetStripeMaxRowCount()
    {
        // Setup
        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setStripeMaxRowCount(1);

        // Verify the results
    }

    @Test
    public void testGetRowGroupMaxRowCount()
    {
        // Setup
        // Run the test
        final int result = orcWriterConfigUnderTest.getRowGroupMaxRowCount();
    }

    @Test
    public void testSetRowGroupMaxRowCount()
    {
        // Setup
        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setRowGroupMaxRowCount(1);

        // Verify the results
    }

    @Test
    public void testGetDictionaryMaxMemory()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcWriterConfigUnderTest.getDictionaryMaxMemory();
    }

    @Test
    public void testSetDictionaryMaxMemory()
    {
        // Setup
        final DataSize dictionaryMaxMemory = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setDictionaryMaxMemory(dictionaryMaxMemory);

        // Verify the results
    }

    @Test
    public void testGetStringStatisticsLimit()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcWriterConfigUnderTest.getStringStatisticsLimit();
    }

    @Test
    public void testSetStringStatisticsLimit()
    {
        // Setup
        final DataSize stringStatisticsLimit = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setStringStatisticsLimit(stringStatisticsLimit);

        // Verify the results
    }

    @Test
    public void testGetMaxCompressionBufferSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = orcWriterConfigUnderTest.getMaxCompressionBufferSize();
    }

    @Test
    public void testSetMaxCompressionBufferSize()
    {
        // Setup
        final DataSize maxCompressionBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setMaxCompressionBufferSize(maxCompressionBufferSize);

        // Verify the results
    }

    @Test
    public void testIsUseLegacyVersion()
    {
        // Setup
        // Run the test
        final boolean result = orcWriterConfigUnderTest.isUseLegacyVersion();
    }

    @Test
    public void testSetUseLegacyVersion()
    {
        // Setup
        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setUseLegacyVersion(false);

        // Verify the results
    }

    @Test
    public void testGetWriterIdentification()
    {
        // Setup
        // Run the test
        final OrcWriterOptions.WriterIdentification result = orcWriterConfigUnderTest.getWriterIdentification();
    }

    @Test
    public void testSetWriterIdentification()
    {
        // Setup
        // Run the test
        final OrcWriterConfig result = orcWriterConfigUnderTest.setWriterIdentification(
                OrcWriterOptions.WriterIdentification.LEGACY_HIVE_COMPATIBLE);

        // Verify the results
    }

    @Test
    public void testSetDefaultBloomFilterFpp()
    {
        double defaultBloomFilterFpp = orcWriterConfigUnderTest.getDefaultBloomFilterFpp();
        orcWriterConfigUnderTest.setDefaultBloomFilterFpp(defaultBloomFilterFpp);
    }

    @Test
    public void testGetValidationPercentage()
    {
        double validationPercentage = orcWriterConfigUnderTest.getValidationPercentage();
        orcWriterConfigUnderTest.setValidationPercentage(validationPercentage);
    }

    @Test
    public void testGetValidationMode()
    {
        OrcWriteValidation.OrcWriteValidationMode validationMode = orcWriterConfigUnderTest.getValidationMode();
        orcWriterConfigUnderTest.setValidationMode(validationMode);
    }
}
