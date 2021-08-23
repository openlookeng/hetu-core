/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.type.InternalTypeManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestKylinClient
{
    public static final JdbcTypeHandle JDBC_BIGINT = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 8, 0, Optional.empty());
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());
    private static final TypeManager TYPE_MANAGER = new InternalTypeManager(createTestMetadataManager().getFunctionAndTypeManager());
    private static final JdbcColumnHandle BIGINT_COLUMN = new JdbcColumnHandle("c_bigint",
            new JdbcTypeHandle(Types.BIGINT, Optional.of("int8"), 0, 0, Optional.empty()), BIGINT, true);
    KylinClient kylinClient = new KylinClient(new BaseJdbcConfig(), new KylinConfig(), identity -> {
        throw new UnsupportedOperationException();
    }, TYPE_MANAGER);
    private TestingDatabase database;
    private final String user = "admin";
    private final String pwd = "admin";

    @BeforeMethod
    public void setup()
            throws SQLException
    {
        database = new TestingDatabase(user, pwd);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        database.close();
    }

    /*@Test
    public void testSelect() {
        KylinSqlWriter kylinSqlWriter = new KylinSqlWriter();
        Selection s1 = new Selection("name");
        Selection s2 = new Selection("age");
        List<Selection> list = new ArrayList<>();
        list.add(s1);
        list.add(s2);
        kylinSqlWriter.select(list, "test");
        assertEquals(kylinSqlWriter.select(list, "test"), "(SELECT name, age FROM /*partstart test /*tablename=test  )");
        assertEquals(kylinSqlWriter.select(list, "SELECT 'abc', 20"), "(SELECT name, age FROM /*partstart SELECT 'abc', 20 /*tablename=SELECT 'abc', 20  )");
    }*/

    @Test
    public void testBuildSql()
            throws SQLException
    {
//        Connection connection = database.getConnection();
//        try (PreparedStatement preparedStatement = connection.prepareStatement("create table \"TESTXYZ\" ("
//                + ""
//                + "\"COL_0\" BIGINT "
//                + ")")) {
//            preparedStatement.execute();
//        }
//        List<JdbcColumnHandle> columns = ImmutableList.of(
//                new JdbcColumnHandle("COL_0", JDBC_BIGINT, BIGINT, true));
//
//        SchemaTableName schemaTableName = new SchemaTableName("default", "TESTXYZ");
//        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(schemaTableName, "", "", "TESTXYZ");
//        PreparedStatement preparedStatement = null;
//        try {
//            preparedStatement = kylinClient.buildSql(SESSION, connection, new JdbcSplit(Optional.empty()), jdbcTableHandle, columns);
//        }
//        catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//        PreparedStatement expected = null;
//        try {
//            expected = connection.prepareStatement("SELECT COL_0 AS COL_0" + " FROM " + "TESTXYZ");
//            assertEquals(preparedStatement.toString().substring(preparedStatement.toString().indexOf(":") + 1), expected.toString().substring(expected.toString().indexOf(":") + 1));
//        }
//        catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//        finally {
//            if (expected != null) {
//                expected.close();
//            }
//        }
    }

    /*@Test
    public void testIsSubQueryPushdownSupported() {
        assertEquals(kylinClient.isSubQueryPushdownSupported(SESSION), true);
    }

    @Test
    public void testImplementAggregation() {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        assertEquals(kylinClient.implementAggregation(new AggregateFunction("sum", BIGINT, ImmutableList.of(bigintVariable), ImmutableList.of(), true, Optional.empty()),
                ImmutableMap.of(bigintVariable.getName(), BIGINT_COLUMN)), Optional.empty());
    }

    @Test
    public void testGetSqlQueryWriter() {
        boolean actual = false;
        if (kylinClient.getSqlQueryWriter().get() instanceof KylinSqlWriter) {
            actual = true;
        }
        assertEquals(actual, true);
    }*/

    @Test
    public void testIsLimitGuaranteed()
    {
        assertEquals(kylinClient.isLimitGuaranteed(), true);
    }

    @Test
    public void testLimitFunction()
    {
        assertEquals(kylinClient.limitFunction().isPresent(), true);
    }

    @Test
    public void testGetSessionProperty()
    {
        assertEquals(Optional.ofNullable(KylinUtil.getSessionProperty(SESSION, "query_pushdown", Boolean.class, true)), Optional.of(true));
    }
}
