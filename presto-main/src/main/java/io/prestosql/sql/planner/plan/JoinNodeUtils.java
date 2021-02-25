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
package io.prestosql.sql.planner.plan;

import io.prestosql.spi.plan.JoinNode;
import io.prestosql.sql.tree.Join;

public class JoinNodeUtils
{
    private JoinNodeUtils() {}

    public static JoinNode.Type typeConvert(Join.Type joinType)
    {
        switch (joinType) {
            case CROSS:
            case IMPLICIT:
            case INNER:
                return JoinNode.Type.INNER;
            case LEFT:
                return JoinNode.Type.LEFT;
            case RIGHT:
                return JoinNode.Type.RIGHT;
            case FULL:
                return JoinNode.Type.FULL;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }
}
