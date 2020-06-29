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

package io.hetu.core.plugin.carbondata;

import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.BaseStorageFormat;
import org.apache.carbondata.hive.CarbonHiveSerDe;
import org.apache.carbondata.hive.MapredCarbonInputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;

import static java.util.Objects.requireNonNull;

public enum CarbondataStorageFormat
        implements BaseStorageFormat
{
    CARBON(
            CarbonHiveSerDe.class.getName(),
            MapredCarbonInputFormat .class.getName(),
            MapredCarbonOutputFormat .class.getName(),
            new DataSize(256.0D, DataSize.Unit.MEGABYTE));

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;
    private final DataSize estimatedWriterSystemMemoryUsage;

    CarbondataStorageFormat(String serde, String inputFormat, String outputFormat, DataSize estimatedWriterSystemMemoryUsage)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
        this.estimatedWriterSystemMemoryUsage = requireNonNull(estimatedWriterSystemMemoryUsage, "estimatedWriterSystemMemoryUsage is null");
    }

    public String getSerDe()
    {
        return serde;
    }

    public String getInputFormat()
    {
        return inputFormat;
    }

    public String getOutputFormat()
    {
        return outputFormat;
    }

    public DataSize getEstimatedWriterSystemMemoryUsage()
    {
        return estimatedWriterSystemMemoryUsage;
    }
}
