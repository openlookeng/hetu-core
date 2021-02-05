/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.oracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.hetu.core.plugin.oracle.config.RoundingMode;
import io.hetu.core.plugin.oracle.config.UnsupportedTypeHandling;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * To get the custom properties to connect to the database. User, password and
 * URL is provided by de BaseJdbcClient is not required. If there is another
 * custom configuration it should be put in here.
 *
 * @since 2019-07-06
 */

public class OracleConfig
{
    private static final int MAX_DEFAULT_SCALE = 38;

    private static final int DEFAULT_SCALE = 0;

    private UnsupportedTypeHandling unsupportedTypeHandling = UnsupportedTypeHandling.FAIL;

    private RoundingMode roundingMode = RoundingMode.UNNECESSARY;

    private int numberDefaultScale = DEFAULT_SCALE;

    private boolean synonymsEnabled;

    public UnsupportedTypeHandling getUnsupportedTypeHandling()
    {
        return unsupportedTypeHandling;
    }

    /**
     * set unsupportedTypeHandling
     *
     * @param unsupportedTypeHandling config from properties
     * @return oracle config object
     */
    @Config("unsupported-type.handling-strategy")
    @ConfigDescription("Configures how unsupported column data types should be handled")
    public OracleConfig setUnsupportedTypeHandling(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        this.unsupportedTypeHandling = unsupportedTypeHandling;
        return this;
    }

    public RoundingMode getRoundingMode()
    {
        return roundingMode;
    }

    /**
     * set Round mode
     *
     * @param roundingMode config from properties
     * @return oracle object
     */
    @Config("oracle.number.rounding-mode")
    @ConfigDescription("Rounding mode for Oracle NUMBER data type. "
            + "This is useful when Oracle NUMBER data type specifies higher scale than is supported in Hetu")
    public OracleConfig setRoundingMode(RoundingMode roundingMode)
    {
        this.roundingMode = roundingMode;
        return this;
    }

    @Min(0)
    @Max(MAX_DEFAULT_SCALE)
    public int getNumberDefaultScale()
    {
        return numberDefaultScale;
    }

    /**
     * set number default scale
     *
     * @param numberDefaultScale config from properties
     * @return oracle config object
     */
    @Config("oracle.number.default-scale")
    @ConfigDescription("Default Hetu DECIMAL scale for Oracle NUMBER (without precision and scale) data type. "
            + "When not set then such column will be treated as not supported")
    public OracleConfig setNumberDefaultScale(Integer numberDefaultScale)
    {
        this.numberDefaultScale = numberDefaultScale;
        return this;
    }

    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }

    /**
     * set oracle synonyms enabled
     *
     * @param enabled config from properties
     * @return oracle config object
     */
    @Config("oracle.synonyms.enabled")
    public OracleConfig setSynonymsEnabled(boolean enabled)
    {
        this.synonymsEnabled = enabled;
        return this;
    }
}
