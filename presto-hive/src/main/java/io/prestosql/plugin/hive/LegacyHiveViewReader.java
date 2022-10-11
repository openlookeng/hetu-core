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

import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorViewDefinition;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.HiveToTrinoTranslator.translateHiveViewToTrino;

public class LegacyHiveViewReader
        implements ViewReaderUtil.ViewReader
{
    @Override
    public ConnectorViewDefinition decodeViewData(String viewData, Table table, CatalogName catalogName)
    {
        String viewText = table.getViewExpandedText()
                .orElseThrow(() -> new PrestoException(HIVE_INVALID_METADATA, "No view expanded text: " + table.getSchemaTableName()));
        return new ConnectorViewDefinition(
                translateHiveViewToTrino(viewText),
                Optional.of(catalogName.toString()),
                Optional.ofNullable(table.getDatabaseName()),
                table.getDataColumns().stream()
                        .map(column -> new ConnectorViewDefinition.ViewColumn(column.getName(), column.getType().getTypeSignature()))
                        .collect(toImmutableList()),
                Optional.ofNullable(table.getOwner()),
                false); // don't run as invoker
    }
}
