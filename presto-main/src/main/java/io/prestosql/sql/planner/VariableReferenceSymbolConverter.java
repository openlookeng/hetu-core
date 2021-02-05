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
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VariableReferenceSymbolConverter
{
    private VariableReferenceSymbolConverter() {}

    public static VariableReferenceExpression toVariableReference(Symbol symbol, TypeProvider typeProvider)
    {
        return new VariableReferenceExpression(symbol.getName(), typeProvider.get(symbol));
    }

    public static List<VariableReferenceExpression> toVariableReferences(List<Symbol> symbols, TypeProvider typeProvider)
    {
        List<VariableReferenceExpression> variableReferences = new ArrayList<>();
        symbols.forEach(symbol -> variableReferences.add(toVariableReference(symbol, typeProvider)));
        return variableReferences;
    }

    public static Map<VariableReferenceExpression, RowExpression> toVariableReferenceMap(Map<Symbol, RowExpression> map, TypeProvider typeProvider)
    {
        Map<VariableReferenceExpression, RowExpression> ret = new LinkedHashMap<>();

        for (Map.Entry<Symbol, RowExpression> entry : map.entrySet()) {
            ret.put(toVariableReference(entry.getKey(), typeProvider), entry.getValue());
        }

        return ret;
    }

    public static VariableReferenceExpression toVariableReference(Symbol symbol, Type type)
    {
        return new VariableReferenceExpression(symbol.getName(), type);
    }

    public static Symbol toSymbol(VariableReferenceExpression expression)
    {
        return new Symbol(expression.getName());
    }
}
