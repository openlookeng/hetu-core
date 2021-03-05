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
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public final class CallExpression
        extends RowExpression
{
    private final Signature signature;
    private final Type returnType;
    private final List<RowExpression> arguments;
    private final Optional<RowExpression> filter;

    @JsonCreator
    public CallExpression(
            @JsonProperty("signature") Signature signature,
            @JsonProperty("returnType") Type returnType,
            @JsonProperty("arguments") List<RowExpression> arguments,
            @JsonProperty("filter") Optional<RowExpression> filter)
    {
        requireNonNull(signature, "signature is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(filter, "filter object is null");

        this.signature = signature;
        this.returnType = returnType;
        this.arguments = ImmutableList.copyOf(arguments);
        this.filter = filter;
    }

    @JsonProperty
    public Signature getSignature()
    {
        return signature;
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
        return signature.getName() + "(" + Joiner.on(", ").join(arguments) + ")";
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
        CallExpression that = (CallExpression) o;
        return Objects.equals(signature, that.signature) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, returnType, arguments);
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
            OperatorType operator = this.getSignature().unmangleOperator(that.getSignature().getName());
            OperatorType operatorThat = that.getSignature().unmangleOperator(that.getSignature().getName());
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
