/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import io.hetu.core.plugin.oracle.config.RoundingMode;
import io.hetu.core.plugin.oracle.config.UnsupportedTypeHandling;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static org.testng.Assert.assertEquals;

/**
 * TestOracleConfig
 *
 * @since 2019-07-08
 */

public class TestOracleConfig
{
    private static final int NUMBER_DEFAULT_SCALE = 2;

    private static final int NUMBER_DEFAULT_SCALE_ASSERT = 0;

    /**
     * testOraclePropertyMappings
     */
    @Test
    public void testOraclePropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("oracle.number.default-scale", "2")
                .put("oracle.number.rounding-mode", "DOWN")
                .put("unsupported-type.handling-strategy", "CONVERT_TO_VARCHAR")
                .put("oracle.synonyms.enabled", "true")
                .build();

        OracleConfig expected = new OracleConfig()
                .setNumberDefaultScale(NUMBER_DEFAULT_SCALE)
                .setRoundingMode(RoundingMode.DOWN)
                .setUnsupportedTypeHandling(UnsupportedTypeHandling.CONVERT_TO_VARCHAR)
                .setSynonymsEnabled(true);

        assertFullMapping(properties, expected);
    }

    /**
     * testGetFuncions
     */
    @Test
    public void testGetFunctions()
    {
        OracleConfig config = new OracleConfig();
        RoundingMode roundingMode = config.getRoundingMode();
        UnsupportedTypeHandling typeHandling = config.getUnsupportedTypeHandling();
        int defaultScale = config.getNumberDefaultScale();

        assertEquals(defaultScale, NUMBER_DEFAULT_SCALE_ASSERT);
        assertEquals(roundingMode, RoundingMode.UNNECESSARY);
        assertEquals(typeHandling, UnsupportedTypeHandling.FAIL);
    }
}
