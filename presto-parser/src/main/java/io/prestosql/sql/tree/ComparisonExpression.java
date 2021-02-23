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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ComparisonExpression
        extends Expression
{
    private final Operator operator;
    private Expression left;
    private Expression right;

    public ComparisonExpression(Operator operator, Expression left, Expression right)
    {
        this(Optional.empty(), operator, left, right);
    }

    public ComparisonExpression(NodeLocation location, Operator operator, Expression left, Expression right)
    {
        this(Optional.of(location), operator, left, right);
    }

    private ComparisonExpression(Optional<NodeLocation> location, Operator operator, Expression left, Expression right)
    {
        super(location);
        requireNonNull(operator, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    public void setRight(Expression right)
    {
        this.right = right;
    }

    public void setLeft(Expression left)
    {
        this.left = left;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitComparisonExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
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

        ComparisonExpression that = (ComparisonExpression) o;
        return (operator == that.operator) &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, left, right);
    }

    public ComparisonExpression flip()
    {
        return new ComparisonExpression(operator.flip(), right, left);
    }

    public enum Operator
    {
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">="),
        IS_DISTINCT_FROM("IS DISTINCT FROM");

        private final String value;

        Operator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        public Operator flip()
        {
            switch (this) {
                case EQUAL:
                    return EQUAL;
                case NOT_EQUAL:
                    return NOT_EQUAL;
                case LESS_THAN:
                    return GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return IS_DISTINCT_FROM;
                default:
                    throw new IllegalArgumentException("Unsupported comparison: " + this);
            }
        }

        public Operator negate()
        {
            switch (this) {
                case EQUAL:
                    return NOT_EQUAL;
                case NOT_EQUAL:
                    return EQUAL;
                case LESS_THAN:
                    return GREATER_THAN_OR_EQUAL;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN;
                case GREATER_THAN:
                    return LESS_THAN_OR_EQUAL;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN;
                default:
                    throw new IllegalArgumentException("Unsupported comparison: " + this);
            }
        }
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ComparisonExpression that = (ComparisonExpression) o;
        Expression tempLeft = left;
        Expression tempThatLeft = that.left;
        if (tempLeft instanceof SymbolReference && tempThatLeft instanceof SymbolReference) {
            tempLeft = new SymbolReference(getActualColName(((SymbolReference) tempLeft).getName()));
            tempThatLeft = new SymbolReference(getActualColName(((SymbolReference) tempThatLeft).getName()));
        }

        // Need to check for right incase expr is 5=id
        return (operator == that.operator) &&
                Objects.equals(tempLeft, tempThatLeft) &&
                Objects.equals(right, that.right);
    }
}
