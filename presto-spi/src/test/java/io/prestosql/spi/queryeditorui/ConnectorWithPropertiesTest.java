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
package io.prestosql.spi.queryeditorui;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

public class ConnectorWithPropertiesTest
{
    private ConnectorWithProperties connectorWithPropertiesUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        connectorWithPropertiesUnderTest = new ConnectorWithProperties("connectorName", Optional.of("value"), false,
                false, false,
                Arrays.asList(
                        new ConnectorWithProperties.Properties("name", "value", "description", Optional.of("value"),
                                Optional.of(false), Optional.of(false))));
    }

    @Test
    public void testAddProperties() throws Exception
    {
        // Setup
        final ConnectorWithProperties.Properties properties = new ConnectorWithProperties.Properties("name", "value",
                "description", Optional.of("value"), Optional.of(false), Optional.of(false));

        // Run the test
        connectorWithPropertiesUnderTest.addProperties(properties);

        // Verify the results
    }
}
