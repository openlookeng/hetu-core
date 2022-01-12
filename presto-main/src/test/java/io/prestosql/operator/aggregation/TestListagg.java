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
package io.prestosql.operator.aggregation;

import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestListagg
{
    private static final Metadata metadata = createTestMetadataManager();
    List<TypeSignature> typeSignatures = TypeSignatureProvider.fromTypes(VARCHAR, VARCHAR, BOOLEAN, VARCHAR, BOOLEAN).stream()
            .map(typeSignatureProvider -> typeSignatureProvider.getTypeSignature()).collect(Collectors.toList());
    InternalAggregationFunction bigIntAgg = metadata.getFunctionAndTypeManager().getAggregateFunctionImplementation(
            new Signature(QualifiedObjectName.valueOfDefaultFunction("listagg"), AGGREGATE, parseTypeSignature("array(bigint)"), typeSignatures));

    @Test
    public void testEmpty()
    {
        assertAggregation(
                bigIntAgg,
                null,
                createStringsBlock(new String[] {null}),
                createStringsBlock(","),
                createBooleansBlock(true),
                createStringsBlock("..."),
                createBooleansBlock(false));
    }

    @Test
    public void testOnlyNullValues()
    {
        assertAggregation(
                bigIntAgg,
                null,
                createStringsBlock(null, null, null),
                createStringsBlock(Collections.nCopies(3, ",")),
                createBooleansBlock(Collections.nCopies(3, true)),
                createStringsBlock(Collections.nCopies(3, "...")),
                createBooleansBlock(Collections.nCopies(3, true)));
    }

    @Test
    public void testOneValue()
    {
        assertAggregation(
                bigIntAgg,
                "value",
                createStringsBlock("value"),
                createStringsBlock(","),
                createBooleansBlock(true),
                createStringsBlock("..."),
                createBooleansBlock(false));
    }

    @Test
    public void testTwoValues()
    {
        assertAggregation(
                bigIntAgg,
                "value1,value2",
                createStringsBlock("value1", "value2"),
                createStringsBlock(Collections.nCopies(2, ",")),
                createBooleansBlock(Collections.nCopies(2, true)),
                createStringsBlock(Collections.nCopies(2, "...")),
                createBooleansBlock(Collections.nCopies(2, true)));
    }

    @Test
    public void testTwoValuesMixedWithNullValues()
    {
        assertAggregation(
                bigIntAgg,
                "value1,value2",
                createStringsBlock(null, "value1", null, "value2", null),
                createStringsBlock(Collections.nCopies(5, ",")),
                createBooleansBlock(Collections.nCopies(5, true)),
                createStringsBlock(Collections.nCopies(5, "...")),
                createBooleansBlock(Collections.nCopies(5, true)));
    }

    @Test
    public void testTwoValuesWithDefaultDelimiter()
    {
        assertAggregation(
                bigIntAgg,
                "value1value2",
                createStringsBlock("value1", "value2"),
                createStringsBlock(Collections.nCopies(2, "")),
                createBooleansBlock(Collections.nCopies(2, true)),
                createStringsBlock(Collections.nCopies(2, "...")),
                createBooleansBlock(Collections.nCopies(2, true)));
    }
}
