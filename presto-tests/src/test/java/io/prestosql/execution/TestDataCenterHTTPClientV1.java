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
package io.prestosql.execution;

import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.client.DataCenterClientSession;
import io.prestosql.client.DataCenterStatementClient;
import io.prestosql.client.StatementClient;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.tests.DistributedQueryRunner;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.client.StatementClientFactory.newStatementClient;
import static io.prestosql.spi.type.UnknownType.UNKNOWN;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public class TestDataCenterHTTPClientV1
{
    private final DistributedQueryRunner queryRunner;
    private TypeManager typeManager = new TypeManager()
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            return UNKNOWN;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return UNKNOWN;
        }

        @Override
        public List<Type> getTypes()
        {
            return Collections.emptyList();
        }

        @Override
        public Collection<ParametricType> getParametricTypes()
        {
            return Collections.emptyList();
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            return Optional.empty();
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            return Optional.empty();
        }

        @Override
        public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            return null;
        }
    };

    public TestDataCenterHTTPClientV1()
            throws Exception
    {
        this.queryRunner = createQueryRunner(TEST_SESSION);
    }

    @Test
    public void testFinalTpchQuery1InfoSetOnAbort()
    {
        String queryString = "SELECT * from tpch.tiny.customer limit 10";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery2InfoSetOnAbort()
    {
        String queryString = "SELECT returnflag, linestatus, sum(quantity) AS sum_qty, sum(extendedprice) AS sum_base_price, " +
                "sum(extendedprice * (1 - discount)) AS sum_disc_price, sum(extendedprice * (1 - discount) * (1 + tax)) " +
                "AS sum_charge, avg(quantity) AS avg_qty, avg(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) " +
                "AS count_order FROM tpch.tiny.lineitem WHERE shipdate <= date '1998-09-16' GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpcdsQuery3InfoSetOnAbort()
    {
        String queryString = "SELECT regionkey, name, CASE WHEN regionkey = 0 THEN 'Africa' ELSE '' END FROM tpch.tiny.region";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery4InfoSetOnAbort()
    {
        String queryString = "SELECT sum(extendedprice * discount) AS revenue FROM tpch.tiny.lineitem WHERE shipdate >= date '1993-01-01' and shipdate < date '1994-01-01' " +
                "and discount between 0.06 - 0.01 and 0.06 + 0.01 and quantity < 25";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery5InfoSetOnAbort()
    {
        String queryString = "SELECT orderkey, orderstatus, orderpriority FROM tpch.tiny.orders " +
                "GROUP BY grouping sets (orderkey, (orderstatus, orderpriority)) LIMIT 10";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery6InfoSetOnAbort()
    {
        String queryString = "SELECT regionkey, name, rank() OVER (PARTITION BY name ORDER BY name DESC) AS rnk FROM tpch.tiny.region ORDER BY name";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery7InfoSetOnAbort()
    {
        String queryString = "SELECT (totalprice + 2) AS new_price FROM tpch.tiny.orders";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery8InfoSetOnAbort()
    {
        String queryString = "SELECT * FROM tpch.tiny.orders WHERE orderdate - interval '29' day > timestamp '2012-10-31 01:00 UTC'";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery9PrepareInfoSetOnAbort()
    {
        String queryString = "PREPARE my_query FROM SELECT * FROM tpch.tiny.customer limit 10";
        assertQuery(queryString);
    }

    @Test
    public void testFinalTpchQuery10DescribeInfoSetOnAbort()
    {
        String queryString = "DESCRIBE tpch.tiny.customer";
        assertQuery(queryString);
    }

    @Test
    public void testFinalShowCatalogsQueryInfoSetOnAbort()
    {
        String queryString = "SHOW CATALOGS";
        assertQuery(queryString);
    }

    @Test
    public void testFinalShowSchemasQueryInfoSetOnAbort()
    {
        String queryString = "SHOW SCHEMAS from tpch";
        assertQuery(queryString);
    }

    private void assertQuery(String sql)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            DataCenterClientSession clientSession = DataCenterClientSession.builder(queryRunner.getCoordinator().getBaseUrl(), "user")
                    .withClientTimeout(new Duration(2, MINUTES))
                    .withTypeManager(typeManager)
                    .build();

            StatementClient client1 = DataCenterStatementClient.newStatementClient(httpClient, clientSession, sql, UUID.randomUUID().toString());

            long client1Count = 0;
            // wait for query to be fully scheduled
            while (client1.isRunning()) {
                if (client1.currentData().getData() != null) {
                    for (List<Object> row : client1.currentData().getData()) {
                        System.out.println(row);
                        client1Count++;
                    }
                }
                client1.advance();
            }
            System.out.println("ROWS: " + client1Count);

            StatementClient client2 = newStatementClient(httpClient, clientSession, sql);

            long client2Count = 0;
            // wait for query to be fully scheduled
            while (client2.isRunning()) {
                if (client2.currentData().getData() != null) {
                    for (List<Object> row : client2.currentData().getData()) {
                        System.out.println(row);
                        client2Count++;
                    }
                }
                client2.advance();
            }
            System.out.println("ROWS: " + client2Count);
            assertEquals(client1Count, client2Count);
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
