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
package io.prestosql.spi.statestore.listener;

import io.prestosql.spi.statestore.Member;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class AbstractMapEventTest
{
    @Mock
    private Member mockMember;

    private AbstractMapEvent abstractMapEventUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractMapEventUnderTest = new AbstractMapEvent(mockMember, 0) {
        };
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", abstractMapEventUnderTest.toString());
    }
}
