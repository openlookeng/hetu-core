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
package io.prestosql.plugin.hive.acid;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;

public class AcidSchemaTest
{
    @Test
    public void test()
    {
        // Run the test
        RowType acidRowIdRowType = AcidSchema.ACID_ROW_ID_ROW_TYPE;
    }

    @Test
    public void testCreateAcidSchema()
    {
        // Run the test
        AcidSchema.createAcidSchema(HIVE_STRING);
    }

    @Test
    public void testCreateRowType()
    {
        // Run the test
        final Type result = AcidSchema.createRowType(Arrays.asList("value"), Arrays.asList(BigintType.BIGINT));
    }

    @Test
    public void testCreateAcidColumnHiveTypes()
    {
        AcidSchema.createAcidColumnHiveTypes(HIVE_STRING);
    }

    @Test
    public void testCreateAcidColumnPrestoTypes()
    {
        AcidSchema.createAcidColumnPrestoTypes(BigintType.BIGINT);
    }
}
