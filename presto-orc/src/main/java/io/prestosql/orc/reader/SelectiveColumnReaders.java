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
                    throw new IllegalArgumentException("Unsupported type: LONG DECIMAL");
                }
                return new ShortDecimalSelectiveColumnReader(orcType, column, filter, outputType, systemMemoryContext.newLocalMemoryContext(SelectiveColumnReaders.class.getSimpleName()));
            case BYTE:
            case FLOAT:
            case DOUBLE:
            case LIST:
            case STRUCT:
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + column.getColumnType());
        }
    }

    public static SelectiveColumnReader wrapWithCachingStreamReader(SelectiveColumnReader original,
                                                                    OrcColumn column,
                                                                    OrcPredicate predicate,
                                                                    Cache<OrcRowDataCacheKey, Block> cache)
    {
        return new SelectiveCachingColumnReader(cache, original, column, predicate);
    }
}
