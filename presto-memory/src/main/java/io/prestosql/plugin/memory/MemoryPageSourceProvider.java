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
package io.prestosql.plugin.memory;

import io.prestosql.plugin.memory.data.MemoryTableManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class MemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final MemoryTableManager pagesStore;

    @Inject
    public MemoryPageSourceProvider(MemoryTableManager pagesStore, TypeManager typeManager, MemoryMetadata memoryMetadata)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        return createPageSource(transaction, session, split, table, columns, Optional.empty());
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier)
    {
        MemorySplit memorySplit = (MemorySplit) split;
        long tableId = memorySplit.getTable();
        int logicalPartNumber = memorySplit.getLogicalPartNum();
        long expectedRows = memorySplit.getExpectedRows();
        MemoryTableHandle memoryTable = (MemoryTableHandle) table;
        OptionalDouble sampleRatio = memoryTable.getSampleRatio();

        TupleDomain<ColumnHandle> predicate = memoryTable.getPredicate();
        // Commenting for Dynamic filter changes

        List<Integer> columnIndexes = columns.stream()
                .map(MemoryColumnHandle.class::cast)
                .map(MemoryColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = pagesStore.getPages(
                tableId,
                logicalPartNumber,
                columnIndexes,
                expectedRows,
                memorySplit.getLimit(),
                sampleRatio,
                predicate);

        if (dynamicFilterSupplier.isPresent()) {
            return new FixedPageSource(pages.stream()
                    .map(page -> applyFilter(page, dynamicFilterSupplier, columns))
                    .collect(toList()));
        }
        else {
            return new FixedPageSource(pages);
        }
    }

    private Page applyFilter(Page page, Optional<DynamicFilterSupplier> dynamicFilters, List<ColumnHandle> columns)
    {
        if (!dynamicFilters.isPresent()) {
            return page;
        }
        int[] positions = new int[page.getPositionCount()];
        int length = 0;
        for (int i = 0; i < page.getPositionCount(); ++i) {
            boolean union = false;
            for (Map<ColumnHandle, DynamicFilter> filter : dynamicFilters.get().getDynamicFilters()) {
                boolean match = true;
                for (Map.Entry<ColumnHandle, DynamicFilter> entry : filter.entrySet()) {
                    MemoryColumnHandle columnHandle = (MemoryColumnHandle) entry.getKey();
                    DynamicFilter dynamicFilter = entry.getValue();
                    Object value = TypeUtils.readNativeValue(columnHandle.getType(typeManager), page.getBlock(columns.indexOf(columnHandle)), i);
                    if (!dynamicFilter.contains(value)) {
                        match = false;
                    }
                }
                union = union || match;
            }
            if (dynamicFilters.get().getDynamicFilters().isEmpty() || union) {
                positions[length++] = i;
            }
        }
        return page.getPositions(positions, 0, length);
    }
}
