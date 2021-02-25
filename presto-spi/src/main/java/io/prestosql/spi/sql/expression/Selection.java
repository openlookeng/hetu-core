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
package io.prestosql.spi.sql.expression;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class Selection
{
    private final String expression;
    private final String alias;

    public Selection(String name)
    {
        this(name, name);
    }

    public Selection(String expression, String alias)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.alias = requireNonNull(alias, "alias is null");
    }

    public String getExpression()
    {
        return expression;
    }

    public String getAlias()
    {
        return alias;
    }

    public boolean isAliased(boolean caseInsensitive)
    {
        return caseInsensitive ? !this.alias.equals(expression) : !this.alias.toLowerCase(Locale.ENGLISH).equals(expression.toLowerCase(Locale.ENGLISH));
    }

    @Override
    public String toString()
    {
        return expression + " AS " + alias;
    }
}
