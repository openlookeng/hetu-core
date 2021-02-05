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
package io.prestosql.sql.planner;

import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class SymbolUtils
{
    private SymbolUtils() {}

    public static SymbolReference toSymbolReference(Symbol symbol)
    {
        return new SymbolReference(symbol.getName());
    }

    public static Symbol from(Expression expression)
    {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
        return new Symbol(((SymbolReference) expression).getName());
    }

    public static Map<Integer, Symbol> toLayOut(List<Symbol> outputSymbols)
    {
        Map<Integer, Symbol> layout = new LinkedHashMap<>();
        int channel = 0;
        for (Symbol symbol : outputSymbols) {
            layout.put(channel++, symbol);
        }
        return layout;
    }
}
