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
package io.prestosql.sql.builder.functioncall.base;

import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.base.FromBase64CallRewriter;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestFromBase64CallRewriter
{
    @Test
    public void testFromBase64CallRewriter()
    {
        List<String> list = new ArrayList<>();
        list.add("'12A69797965458999E'");
        FunctionCallArgsPackage functionCallArgsPackage =
                new FunctionCallArgsPackage(new QualifiedName(Collections.emptyList()), false, list,
                        Optional.empty(), Optional.empty(), Optional.empty());
        FromBase64CallRewriter fromBase64CallRewriter = new FromBase64CallRewriter();
        assertEquals(fromBase64CallRewriter.rewriteFunctionCall(functionCallArgsPackage), "D7603AF7BF7BF7AE78E7CF7DF4");
    }
}
