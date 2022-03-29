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

import io.airlift.units.DataSize;
import io.prestosql.orc.OrcWriterOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Properties;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.prestosql.plugin.hive.HiveMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.prestosql.plugin.hive.OrcFileWriterFactory.getOrcWriterBloomOptions;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestOrcWriterOptions
{
    @Test
    public void testDefaults()
    {
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions();
        assertThat(orcWriterOptions.getStripeMinSize()).isEqualTo(new DataSize(32, MEGABYTE));
        assertThat(orcWriterOptions.getStripeMaxSize()).isEqualTo(new DataSize(64, MEGABYTE));
        assertThat(orcWriterOptions.getStripeMaxRowCount()).isEqualTo(10_000_000);
        assertThat(orcWriterOptions.getRowGroupMaxRowCount()).isEqualTo(10_000);
        assertThat(orcWriterOptions.getDictionaryMaxMemory()).isEqualTo(new DataSize(16, MEGABYTE));
        assertThat(orcWriterOptions.getMaxStringStatisticsLimit()).isEqualTo(new DataSize(64, BYTE));
        assertThat(orcWriterOptions.getMaxCompressionBufferSize()).isEqualTo(new DataSize(256, KILOBYTE));
        assertThat(orcWriterOptions.getBloomFilterFpp()).isEqualTo(0.05);
        assertThat(orcWriterOptions.isBloomFilterColumn("unknown_bloom_column")).isFalse();
    }

    @Test
    public void testOrcWriterOptionsFromTableProperties()
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(ORC_BLOOM_FILTER_COLUMNS_KEY, "test_column_a, test_column_b");
        tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_KEY, "0.1");
        OrcWriterOptions orcWriterOptions = getOrcWriterBloomOptions(tableProperties, new OrcWriterOptions());

        assertThat(orcWriterOptions.isBloomFilterColumn("test_column_a")).isTrue();
        assertThat(orcWriterOptions.isBloomFilterColumn("test_column_b")).isTrue();
        assertThat(orcWriterOptions.isBloomFilterColumn("unknown_bloom_column")).isFalse();

        assertThat(orcWriterOptions.getBloomFilterFpp()).isEqualTo(0.1);
        assertThat(orcWriterOptions.getBloomFilterFpp()).isNotEqualTo(0.5);
    }

    @Test(dataProvider = "invalidBloomFilterFpp")
    public void testOrcWriterOptionsWithInvalidFPPValue(String fpp)
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(ORC_BLOOM_FILTER_COLUMNS_KEY, "column_with_bloom_filter");
        tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_KEY, fpp);
        assertThatThrownBy(() -> getOrcWriterBloomOptions(tableProperties, new OrcWriterOptions()))
                .hasMessage("Invalid value for orc_bloom_filter_fpp property: " + fpp);
    }

    @Test(dataProvider = "invalidRangeBloomFilterFpp")
    public void testOrcBloomFilterWithInvalidRange(String fpp)
    {
        Properties tableProperties = new Properties();
        tableProperties.setProperty(ORC_BLOOM_FILTER_COLUMNS_KEY, "column_with_bloom_filter");
        tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_KEY, fpp);
        assertThatThrownBy(() -> getOrcWriterBloomOptions(tableProperties, new OrcWriterOptions()))
                .hasMessage("bloomFilterFpp should be > 0.0 & < 1.0");
    }

    @DataProvider
    public Object[][] invalidBloomFilterFpp()
    {
        return new Object[][]{
                {"abc"},
                {"12c"},
                {"$"},
                {"*"}
        };
    }

    @DataProvider
    public Object[][] invalidRangeBloomFilterFpp()
    {
        return new Object[][]{
                {"10"},
                {"-10"},
                {"0"},
                {"1"}
        };
    }
}
