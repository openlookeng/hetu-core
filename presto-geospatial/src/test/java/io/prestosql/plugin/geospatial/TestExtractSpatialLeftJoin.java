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
package io.prestosql.plugin.geospatial;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.TestingRowExpressionTranslator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.ExtractSpatialJoins.ExtractSpatialLeftJoin;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.iterative.rule.test.RuleAssert;
import io.prestosql.sql.planner.iterative.rule.test.RuleTester;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static io.prestosql.plugin.geospatial.GeometryType.GEOMETRY;
import static io.prestosql.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static io.prestosql.spi.plan.JoinNode.Type.LEFT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestExtractSpatialLeftJoin
        extends BaseRuleTest
{
    private TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    public TestExtractSpatialLeftJoin()
    {
        super(new GeoPlugin());
    }

    @BeforeClass
    public void setupTranslator()
    {
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(tester().getMetadata());
    }

    @Test
    public void testDoesNotFire()
    {
        // scalar expression
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.symbol("b", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText('POLYGON ((0 0, 0 0, 0 0, 0 0))'), b)",
                                        ImmutableMap.of("b", GEOMETRY))))
                .doesNotFire();

        // OR operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                p.values(p.symbol("point", GEOMETRY), p.symbol("name_2")),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), point) OR name_1 != name_2",
                                        ImmutableMap.of("wkt", VARCHAR, "point", GEOMETRY, "name_1", BIGINT, "name_2", BIGINT))))
                .doesNotFire();

        // NOT operator
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                p.values(p.symbol("point", GEOMETRY), p.symbol("name_2")),
                                sqlToRowExpression(
                                        "NOT ST_Contains(ST_GeometryFromText(wkt), point)",
                                        ImmutableMap.of("wkt", VARCHAR, "point", GEOMETRY, "name_1", BIGINT, "name_2", BIGINT))))
                .doesNotFire();

        // ST_Distance(...) > r
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a", GEOMETRY)),
                                p.values(p.symbol("b", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Distance(a, b) > 5",
                                        ImmutableMap.of("a", GEOMETRY, "b", GEOMETRY))))
                .doesNotFire();

        // SphericalGeography operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a", SPHERICAL_GEOGRAPHY)),
                                p.values(p.symbol("b", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Distance(a, b) < 5",
                                        ImmutableMap.of("a", SPHERICAL_GEOGRAPHY, "b", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(p.symbol("point", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Distance(to_spherical_geography(ST_GeometryFromText(wkt)), point) < 5",
                                        ImmutableMap.of("wkt", VARCHAR, "point", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();
    }

    @Test(enabled = false)
    public void testSphericalGeographiesDoesNotFire()
    {
        // TODO enable once #13133 is merged
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("polygon", SPHERICAL_GEOGRAPHY)),
                                p.values(p.symbol("point", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Contains(polygon, point)",
                                        ImmutableMap.of("polygon", SPHERICAL_GEOGRAPHY, "point", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(p.symbol("point", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Contains(to_spherical_geography(ST_GeometryFromText(wkt)), point)",
                                        ImmutableMap.of("wkt", VARCHAR, "point", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();
    }

    @Test
    public void testConvertToSpatialJoin()
    {
        // symbols
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a", GEOMETRY)),
                                p.values(p.symbol("b", GEOMETRY)),
                                sqlToRowExpression("ST_Contains(a, b)", ImmutableMap.of("a", GEOMETRY, "b", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0)),
                                values(ImmutableMap.of("b", 0))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a", GEOMETRY), p.symbol("name_1")),
                                p.values(p.symbol("b", GEOMETRY), p.symbol("name_2")),
                                sqlToRowExpression("name_1 != name_2 AND ST_Contains(a, b)", ImmutableMap.of("a", GEOMETRY, "b", GEOMETRY, "name_1", BIGINT, "name_2", BIGINT))))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0, "name_1", 1)),
                                values(ImmutableMap.of("b", 0, "name_2", 1))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a1", GEOMETRY), p.symbol("a2", GEOMETRY)),
                                p.values(p.symbol("b1", GEOMETRY), p.symbol("b2", GEOMETRY)),
                                sqlToRowExpression("ST_Contains(a1, b1) AND ST_Contains(a2, b2)", ImmutableMap.of("a1", GEOMETRY, "b1", GEOMETRY, "a2", GEOMETRY, "b2", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(a1, b1) AND ST_Contains(a2, b2)",
                                values(ImmutableMap.of("a1", 0, "a2", 1)),
                                values(ImmutableMap.of("b1", 0, "b2", 1))));
    }

    @Test
    public void testPushDownFirstArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(p.symbol("point", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), point)",
                                        ImmutableMap.of("wkt", VARCHAR, "point", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                values(ImmutableMap.of("point", 0))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), ST_Point(0, 0))",
                                        ImmutableMap.of("wkt", VARCHAR))))
                .doesNotFire();
    }

    @Test
    public void testPushDownSecondArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("polygon", GEOMETRY)),
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                sqlToRowExpression(
                                        "ST_Contains(polygon, ST_Point(lng, lat))",
                                        ImmutableMap.of("polygon", GEOMETRY, "lat", BIGINT, "lng", BIGINT))))
                .matches(
                        spatialLeftJoin("ST_Contains(polygon, st_point)",
                                values(ImmutableMap.of("polygon", 0)),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText('POLYGON ((0 0, 0 0, 0 0, 0 0))'), ST_Point(lng, lat))",
                                        ImmutableMap.of("polygon", GEOMETRY, "lat", BIGINT, "lng", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testPushDownBothArguments()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                                        ImmutableMap.of("wkt", VARCHAR, "lat", BIGINT, "lng", BIGINT))))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));
    }

    @Test
    public void testPushDownOppositeOrder()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                p.values(p.symbol("wkt", VARCHAR)),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                                        ImmutableMap.of("wkt", VARCHAR, "lat", BIGINT, "lng", BIGINT))))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1))),
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0)))));
    }

    @Test
    public void testPushDownAnd()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                p.values(p.symbol("lat"), p.symbol("lng"), p.symbol("name_2")),
                                sqlToRowExpression(
                                        "name_1 != name_2 AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                                        ImmutableMap.of("wkt", VARCHAR, "name_1", BIGINT, "name_2", BIGINT, "lat", BIGINT, "lng", BIGINT))))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0, "name_1", 1))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1, "name_2", 2)))));

        // Multiple spatial functions - only the first one is being processed
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt1", VARCHAR), p.symbol("wkt2", VARCHAR)),
                                p.values(p.symbol("geometry1", GEOMETRY), p.symbol("geometry2", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt1), geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                        ImmutableMap.of("wkt1", VARCHAR, "wkt2", VARCHAR, "geometry1", GEOMETRY, "geometry2", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt1)")), values(ImmutableMap.of("wkt1", 0, "wkt2", 1))),
                                values(ImmutableMap.of("geometry1", 0, "geometry2", 1))));
    }

    private RuleAssert assertRuleApplication()
    {
        RuleTester tester = tester();
        return tester().assertThat(new ExtractSpatialLeftJoin(tester.getMetadata(), tester.getSplitManager(), tester.getPageSourceManager(), tester.getTypeAnalyzer()));
    }

    private RowExpression sqlToRowExpression(String sql, Map<String, Type> typeMap)
    {
        Map<Symbol, Type> types = typeMap.entrySet().stream().collect(Collectors.toMap(e -> new Symbol(e.getKey()), e -> e.getValue()));
        return sqlToRowExpressionTranslator.translateAndOptimize(PlanBuilder.expression(sql), TypeProvider.copyOf(types));
    }
}
