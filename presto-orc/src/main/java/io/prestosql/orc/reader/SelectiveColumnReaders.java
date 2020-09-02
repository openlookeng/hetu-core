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
package io.prestosql.orc.reader;

import com.google.common.cache.Cache;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcRowDataCacheKey;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.Optional;

import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;

public final class SelectiveColumnReaders
{
    private SelectiveColumnReaders() {}

    public static SelectiveColumnReader createColumnReader(
            OrcType orcType,
            OrcColumn column,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        switch (column.getColumnType()) {
            case BOOLEAN:
                return new BooleanSelectiveColumnReader(column, filter, outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongSelectiveColumnReader(column, filter, outputType, systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceSelectiveColumnReader(orcType, column, filter, outputType, systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case TIMESTAMP:
                return new TimestampSelectiveColumnReader(column, filter, hiveStorageTimeZone, outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case DECIMAL:
                if (orcType.getPrecision().get() > MAX_SHORT_PRECISION) {
                    return new LongDecimalSelectiveColumnReader(orcType, column, filter, outputType, systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
                }
                return new ShortDecimalSelectiveColumnReader(orcType, column, filter, outputType, systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case DOUBLE:
                return new DoubleSelectiveColumnReader(column, filter, outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case FLOAT:
                return new FloatSelectiveColumnReader(column, filter, outputType.isPresent(), systemMemoryContext.newLocalMemoryContext(SelectiveColumnReader.class.getSimpleName()));
            case BYTE:
            case LIST:
            case STRUCT:
            case UNION:
            case MAP:
            default:
                throw new IllegalArgumentException("Unsupported type: " + column.getColumnType());
        }
    }

    public static SelectiveColumnReader wrapWithResultCachingStreamReader(SelectiveColumnReader original,
                                                                          OrcColumn column,
                                                                          OrcPredicate predicate,
                                                                          Cache<OrcRowDataCacheKey, Block> cache)
    {
        return new ResultCachingSelectiveColumnReader(cache, original, column, predicate);
    }

    public static SelectiveColumnReader wrapWithDataCachingStreamReader(ColumnReader original,
                                                                        OrcColumn column,
                                                                        Cache<OrcRowDataCacheKey, Block> cache)
    {
        return new DataCachingSelectiveColumnReader(original, column, cache);
    }
}
