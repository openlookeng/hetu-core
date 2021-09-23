/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.hbase.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.hetu.core.metastore.jdbc.JdbcMetastoreModule;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.VarcharType;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.hetu.core.metastore.MetaStoreConstants.LOCAL;
import static io.prestosql.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;

public class TestingHetuMetastore
        implements AutoCloseable
{
    private static final String TEST_DATABASES = "hbase";
    private static final String TEST_MYSQL_USER = "user";
    private static final String TEST_MYSQL_PASSWORD = "testpass";
    private static final HBaseTable TEST_TABLE = new HBaseTable(
            "hbase", "test_table",
            ImmutableList.of(
                    new HBaseColumnHandle("rowkey", Optional.of(""), Optional.of(""), VarcharType.VARCHAR,
                            0, "HBase row ID", false),
                    new HBaseColumnHandle("name", Optional.of("name"), Optional.of("nick_name"), VarcharType.VARCHAR,
                            1, "HBase column name:nick_name. Indexed: false", false),
                    new HBaseColumnHandle("age", Optional.of("age"), Optional.of("lit_age"), BigintType.BIGINT,
                            2, "HBase column age:lit_age. Indexed: false", false),
                    new HBaseColumnHandle("gender", Optional.of("gender"), Optional.of("gender"), DateType.DATE,
                            3, "HBase column gender:gender. Indexed:false", false),
                    new HBaseColumnHandle("t", Optional.of("t"), Optional.of("t"), BigintType.BIGINT,
                            4, "HBase column t:t. Indexed: false", false)),
            "rowkey",
            false,
            Optional.of("io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer"),
            Optional.empty(),
            Optional.of("hbase:test_table"),
            Optional.empty());

    private final TestingMySqlServer mySqlServer;
    private final HetuHBaseMetastore metaStore;

    public TestingHetuMetastore()
    {
        try {
            this.mySqlServer = new TestingMySqlServer(TEST_MYSQL_USER, TEST_MYSQL_PASSWORD, TEST_DATABASES);
            Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                    .put("hetu.metastore.db.url", mySqlServer.getJdbcUrl(TEST_DATABASES))
                    .put("hetu.metastore.db.user", TEST_MYSQL_USER)
                    .put("hetu.metastore.db.password", TEST_MYSQL_PASSWORD)
                    .put("hetu.metastore.cache.ttl", "0s")
                    .build();

            String type = LOCAL;
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new JdbcMetastoreModule(type));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(properties)
                    .initialize();

            this.metaStore = new HetuHBaseMetastore(injector.getInstance(HetuMetastore.class));
            metaStore.init();
            Map<String, ColumnHandle> map = TEST_TABLE.getColumns().stream().collect(Collectors.toMap(
                    HBaseColumnHandle::getColumnName,
                    Function.identity()));
            TEST_TABLE.setColumnsToMap(map);
            metaStore.addHBaseTable(TEST_TABLE);
        }
        catch (Exception ex) {
            throwIfUnchecked(ex);
            throw new PrestoException(HETU_METASTORE_CODE,
                    "init hetu hbase metastore module failed.");
        }
    }

    public HetuHBaseMetastore getHetuMetastore()
    {
        return metaStore;
    }

    @Override
    public void close()
    {
        mySqlServer.close();
    }
}
