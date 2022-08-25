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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HiveApplyProjectionUtilTest
{
    @Mock
    private Type mockType;

    @Test
    public void testExtractSupportedProjectedColumns()
    {
        HiveApplyProjectionUtil.extractSupportedProjectedColumns(new ConnectorExpression(new FieldSetterFactoryTest.Type()));
    }

    public static class ConnectorExpression
            extends io.prestosql.spi.expression.ConnectorExpression
    {
        public ConnectorExpression(Type type)
        {
            super(type);
        }

        @Override
        public String toString()
        {
            return null;
        }

        @Override
        public List<? extends io.prestosql.spi.expression.ConnectorExpression> getChildren()
        {
            ConnectorExpression connectorExpression = new ConnectorExpression(new FieldSetterFactoryTest.Type())
            {
                @Override
                public String toString()
                {
                    return null;
                }

                @Override
                public List<? extends ConnectorExpression> getChildren()
                {
                    return null;
                }
            };
            return Arrays.asList(connectorExpression);
        }
    }

    @Test
    public void testCreateProjectedColumnRepresentation()
    {
        // Setup
        final HiveApplyProjectionUtil.ProjectedColumnRepresentation expectedResult = new HiveApplyProjectionUtil.ProjectedColumnRepresentation(
                new Variable("name", new FieldSetterFactoryTest.Type()), Arrays.asList(0));

        // Run the test
        final HiveApplyProjectionUtil.ProjectedColumnRepresentation result = HiveApplyProjectionUtil.createProjectedColumnRepresentation(
                new ConnectorExpression(new FieldSetterFactoryTest.Type()));
    }

    @Test
    public void testFind() throws Exception
    {
        // Setup
        final Map<String, ColumnHandle> assignments = new HashMap<>();
        final HiveApplyProjectionUtil.ProjectedColumnRepresentation projectedColumn = new HiveApplyProjectionUtil.ProjectedColumnRepresentation(
                new Variable("name", new FieldSetterFactoryTest.Type()), Arrays.asList(0));

        projectedColumn.getDereferenceIndices();
        projectedColumn.isVariable();
        projectedColumn.equals(null);
        projectedColumn.hashCode();

        // Run the test
        final Optional<String> result = HiveApplyProjectionUtil.find(assignments, projectedColumn);
    }

    @Test
    public void testReplaceWithNewVariables()
    {
        ConnectorExpression connectorExpression = new ConnectorExpression(new FieldSetterFactoryTest.Type());
        Map<io.prestosql.spi.expression.ConnectorExpression, Variable> objectObjectHashMap = new HashMap<>();
        Variable name = new Variable("name", new FieldSetterFactoryTest.Type());
        objectObjectHashMap.put(connectorExpression, name);
        HiveApplyProjectionUtil.replaceWithNewVariables(connectorExpression, objectObjectHashMap);
        HiveApplyProjectionUtil.replaceWithNewVariables(new Constant("name", new FieldSetterFactoryTest.Type()), objectObjectHashMap);
        HiveApplyProjectionUtil.replaceWithNewVariables(new FieldDereference(new FieldSetterFactoryTest.Type(), connectorExpression, 1), objectObjectHashMap);
    }
}
