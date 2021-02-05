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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;

import static com.google.common.base.Preconditions.checkArgument;

public class ApplyNodeUtil
{
    private ApplyNodeUtil() {}

    public static void verifySubquerySupported(Assignments assignments)
    {
        checkArgument(
                assignments.getExpressions().stream().allMatch(ApplyNodeUtil::isSupportedSubqueryExpression),
                "Unexpected expression used for subquery expression");
    }

    public static boolean isSupportedSubqueryExpression(RowExpression rowExpression)
    {
        if (OriginalExpressionUtils.isExpression(rowExpression)) {
            Expression expression = OriginalExpressionUtils.castToExpression(rowExpression);
            return expression instanceof InPredicate ||
                    expression instanceof ExistsPredicate ||
                    expression instanceof QuantifiedComparisonExpression;
        }

        if (rowExpression instanceof SpecialForm) {
            return true;
        }
        // TODO: add RowExpression support
        return false;
    }
}
