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
package io.prestosql.spi.relation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class CallExpression
        extends RowExpression
{
    private final String displayName;
    private final FunctionHandle functionHandle;
    private final Type returnType;
    private final List<RowExpression> arguments;
    private final Optional<RowExpression> filter;

    public CallExpression(String displayName, FunctionHandle functionHandle, Type returnType, List<RowExpression> arguments)
    {
        this(displayName, functionHandle, returnType, arguments, Optional.empty());
    }

    @JsonCreator
    public CallExpression(
            @JsonProperty("displayName") String displayName,
            @JsonProperty("functionHandle") FunctionHandle functionHandle,
            @JsonProperty("returnType") Type returnType,
            @JsonProperty("arguments") List<RowExpression> arguments,
            @JsonProperty("filter") Optional<RowExpression> filter)
    {
        requireNonNull(displayName, "displayName is null");
        requireNonNull(functionHandle, "functionHandle is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(filter, "filter object is null");

        this.displayName = displayName;
        this.functionHandle = functionHandle;
        this.returnType = returnType;
        this.arguments = ImmutableList.copyOf(arguments);
        this.filter = filter;
    }

    @JsonProperty
    public String getDisplayName()
    {
        return displayName;
    }

    @JsonProperty
    public FunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @Override
    @JsonProperty("returnType")
    public Type getType()
    {
        return returnType;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public Optional<RowExpression> getFilter()
    {
        return filter;
    }

    @Override
    public String toString()
    {
        return displayName + "(" + Joiner.on(", ").join(arguments) + ")";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CallExpression other = (CallExpression) o;
        return Objects.equals(this.functionHandle, other.functionHandle) && Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionHandle, arguments);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }

    private boolean isInteger(String st)
    {
        try {
            Integer.parseInt(st);
        }
        catch (NumberFormatException ex) {
            return false;
        }

        return true;
    }

    private String getActualColName(String var)
    {
        int index = var.lastIndexOf("_");
        if (index == -1 || isInteger(var.substring(index + 1)) == false) {
            return var;
        }
        else {
            return var.substring(0, index);
        }
    }

    @Override
    public boolean absEquals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !(o instanceof CallExpression)) {
            return false;
        }

        try {
            CallExpression that = (CallExpression) o;
            OperatorType operator = OperatorType.valueOf(this.displayName.toUpperCase(Locale.ENGLISH));
            OperatorType operatorThat = OperatorType.valueOf(that.displayName.toUpperCase(Locale.ENGLISH));
            if (!operator.isComparisonOperator() || !operatorThat.isComparisonOperator() ||
                    this.getArguments().size() != 2 || that.getArguments().size() != 2) {
                return false;
            }
            RowExpression tempLeft = this.getArguments().get(0);
            RowExpression tempThatLeft = that.getArguments().get(0);
            if (tempLeft instanceof VariableReferenceExpression && tempThatLeft instanceof VariableReferenceExpression) {
                tempLeft = new VariableReferenceExpression(getActualColName(((VariableReferenceExpression) tempLeft).getName()), tempLeft.getType());
                tempThatLeft = new VariableReferenceExpression(getActualColName(((VariableReferenceExpression) tempThatLeft).getName()), tempThatLeft.getType());
            }

            // Need to check for right incase expr is 5=id
            return ((operator == operatorThat) &&
                    Objects.equals(tempLeft, tempThatLeft) &&
                    Objects.equals(this.getArguments().get(1), that.getArguments().get(1)));
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }
}
