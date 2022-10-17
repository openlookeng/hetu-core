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
package io.hetu.core.plugin.dm;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.hetu.core.plugin.oracle.OracleConfig;
import io.hetu.core.plugin.oracle.config.RoundingMode;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class DaMengConfig
        extends OracleConfig
{
    private static final int MAX_DEFAULT_SCALE = 38;

    private static final int DEFAULT_SCALE = 0;

    private RoundingMode roundingMode = RoundingMode.UNNECESSARY;

    private int numberDefaultScale = DEFAULT_SCALE;

    public RoundingMode getDaMengRoundingMode()
    {
        return roundingMode;
    }

    /**
     * set Round mode
     *
     * @param roundingMode config from properties
     * @return dameng object
     */
    @Config("dameng.number.rounding-mode")
    @ConfigDescription("Rounding mode for DM NUMBER data type. "
            + "This is useful when dameng NUMBER data type specifies higher scale than is supported in Hetu")
    public DaMengConfig setDaMengRoundingMode(RoundingMode roundingMode)
    {
        this.roundingMode = roundingMode;
        return this;
    }

    @Min(0)
    @Max(MAX_DEFAULT_SCALE)
    public int getDaMengNumberDefaultScale()
    {
        return numberDefaultScale;
    }

    /**
     * set number default scale
     *
     * @param numberDefaultScale config from properties
     * @return dameng config object
     */
    @Config("dameng.number.default-scale")
    @ConfigDescription("Default Hetu DECIMAL scale for DM NUMBER (without precision and scale) data type. "
            + "When not set then such column will be treated as not supported")
    public DaMengConfig setDaMengNumberDefaultScale(Integer numberDefaultScale)
    {
        this.numberDefaultScale = numberDefaultScale;
        return this;
    }
}
