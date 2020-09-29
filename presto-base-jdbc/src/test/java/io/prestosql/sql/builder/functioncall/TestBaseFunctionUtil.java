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
package io.prestosql.sql.builder.functioncall;

import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestBaseFunctionUtil
{
    @Test
    public void testFormatIdentifier()
    {
        String col1String = "col1";
        Selection selection1 = new Selection("col11");
        String col2String = "col2";
        Selection selection2 = new Selection("col22");
        Map<String, Selection> map = new HashMap<>();
        map.put(col1String, selection1);
        map.put(col2String, selection2);
        Optional<Map<String, Selection>> qualifiedNames = Optional.of(map);

        assertEquals(BaseFunctionUtil.formatIdentifier(qualifiedNames, "col1"), "col11");
        assertEquals(BaseFunctionUtil.formatIdentifier(qualifiedNames, "col2"), "col22");
    }

    @Test
    public void testFormatQualifiedName()
    {
        List<String> argsList = new ArrayList<>();
        argsList.add("var1");
        argsList.add("var2");
        QualifiedName qualifiedName = new QualifiedName(argsList);
        assertEquals(BaseFunctionUtil.formatQualifiedName(qualifiedName), "var1.var2");
    }
}
