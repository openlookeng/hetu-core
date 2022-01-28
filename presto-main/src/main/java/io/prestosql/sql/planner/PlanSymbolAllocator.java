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

import com.google.common.primitives.Ints;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PlanSymbolAllocator
        implements SymbolAllocator
{
    private final Map<Symbol, Type> symbols;
    private int nextId;

    public PlanSymbolAllocator()
    {
        symbols = new HashMap<>();
    }

    public PlanSymbolAllocator(Map<Symbol, Type> initial)
    {
        symbols = new HashMap<>(initial);
    }

    public Symbol newSymbol(Symbol symbolHint)
    {
        checkArgument(symbols.containsKey(symbolHint), "symbolHint not in symbols map");
        return newSymbol(symbolHint.getName(), symbols.get(symbolHint));
    }

    @Override
    public Symbol newSymbol(String nameHint, Type type)
    {
        return newSymbol(nameHint, type, null);
    }

    public Symbol newHashSymbol()
    {
        return newSymbol("$hashValue", BigintType.BIGINT);
    }

    @Override
    public Symbol newSymbol(String nameHint, Type type, String suffix)
    {
        requireNonNull(nameHint, "name is null");
        requireNonNull(type, "type is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        String nameHintLower = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHintLower.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHintLower.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (Ints.tryParse(tail) != null || index == nameHintLower.length() - 1) {
                nameHintLower = nameHintLower.substring(0, index);
            }
        }

        String unique = nameHintLower;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }

        String attempt = unique;
        while (symbols.containsKey(new Symbol(attempt))) {
            attempt = unique + "_" + nextId();
        }

        Symbol symbol = new Symbol(attempt);
        symbols.put(symbol, type);
        return symbol;
    }

    public Symbol newSymbol(Expression expression, Type type)
    {
        return newSymbol(expression, type, null);
    }

    public Symbol newSymbol(Expression expression, Type type, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof Identifier) {
            nameHint = ((Identifier) expression).getValue();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix();
        }
        else if (expression instanceof SymbolReference) {
            nameHint = ((SymbolReference) expression).getName();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
        }

        return newSymbol(nameHint, type, suffix);
    }

    public Symbol newSymbol(Field field)
    {
        String nameHint = field.getName().orElse("field");
        return newSymbol(nameHint, field.getType());
    }

    public Symbol newSymbol(RowExpression expression)
    {
        return newSymbol(expression, null);
    }

    public Symbol newSymbol(RowExpression expression, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof VariableReferenceExpression) {
            nameHint = ((VariableReferenceExpression) expression).getName();
        }
        else if (expression instanceof CallExpression) {
            String[] names = ((CallExpression) expression).getDisplayName().split("\\.");
            nameHint = names[names.length - 1];
        }
        return newSymbol(nameHint, expression.getType(), suffix);
    }

    public Map<Symbol, Type> getSymbols()
    {
        return symbols;
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(symbols);
    }

    private int nextId()
    {
        return nextId++;
    }
}
