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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.spi.security.PrincipalType.USER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveView
        extends AbstractTestHiveLocal
{
    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        HiveConfig hiveConfig = new HiveConfig();
        HdfsConfigurationInitializer updator = new HdfsConfigurationInitializer(hiveConfig);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updator, ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
    }

    @Override
    public void testMismatchSchemaTable()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testPartitionSchemaMismatch()
    {
        // test expects an exception to be thrown
        throw new SkipException("FileHiveMetastore only supports replaceTable() for views");
    }

    @Override
    public void testBucketedTableEvolution()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // FileHiveMetastore has various incompatibilities
    }

    @Test(enabled = false)
    public void tesHiveView()
    {
        SchemaTableName temporaryCreateView = temporaryTable("hive_view");
        String viewData = "test hive view";
        String expectedvViewData = "{\n" +
                "  \"originalSql\" : \"test hive view\",\n" +
                "  \"catalog\" : \"hive\",\n" +
                "  \"columns\" : [ {\n" +
                "    \"name\" : \"dummy\",\n" +
                "    \"type\" : \"varchar\"\n" +
                "  } ],\n" +
                "  \"owner\" : \"test\",\n" +
                "  \"runAsInvoker\" : false\n" +
                "}";
        String owner = "test";
        ConnectorSession session = newSession();
        HiveIdentity identity = new HiveIdentity(session);
        metastoreClient.createTable(identity, buildHiveView(temporaryCreateView, owner, viewData), buildInitialPrivilegeSet(owner));
        try (Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            Optional<ConnectorViewDefinition> views = metadata.getView(newSession(), temporaryCreateView);
            assertEquals(views.get().getOriginalSql(), expectedvViewData);

            assertTrue(metadata.listViews(newSession(), Optional.of(temporaryCreateView.getSchemaName())).contains(temporaryCreateView));
        }
        finally {
            metastoreClient.dropTable(identity, temporaryCreateView.getSchemaName(), temporaryCreateView.getTableName(), true);
        }
    }

    private static Table buildHiveView(SchemaTableName viewName, String owner, String viewData)
    {
        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(owner)
                .setTableType(TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(ImmutableMap.of())
                .setViewOriginalText(Optional.of(viewData))
                .setViewExpandedText(Optional.of(viewData));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.VIEW_STORAGE_FORMAT)
                .setLocation("");

        return tableBuilder.build();
    }

    private static PrincipalPrivileges buildInitialPrivilegeSet(String tableOwner)
    {
        HivePrincipal owner = new HivePrincipal(USER, tableOwner);
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.INSERT, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.UPDATE, true, owner, owner))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.DELETE, true, owner, owner))
                        .build(),
                ImmutableMultimap.of());
    }
}
