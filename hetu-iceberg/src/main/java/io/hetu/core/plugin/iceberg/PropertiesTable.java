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
package io.hetu.core.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.hetu.core.plugin.iceberg.util.PageListBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.iceberg.Table;

import java.util.List;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class PropertiesTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    public PropertiesTable(SchemaTableName tableName, Table icebergTable)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        this.tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("key", VARCHAR))
                        .add(new ColumnMetadata("value", VARCHAR))
                        .build());
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(tableMetadata, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        icebergTable.properties().entrySet().forEach(prop -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(prop.getKey());
            pagesBuilder.appendVarchar(prop.getValue());
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }
}
