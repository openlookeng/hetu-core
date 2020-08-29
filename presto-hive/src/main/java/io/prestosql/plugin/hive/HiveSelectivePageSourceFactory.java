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

import io.prestosql.plugin.hive.coercions.HiveCoercer;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public interface HiveSelectivePageSourceFactory
{
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
            List<Integer> outputColumns,
            TupleDomain<HiveColumnHandle> domainPredicate,
            Optional<List<TupleDomain<HiveColumnHandle>>> additionPredicates,
            DateTimeZone hiveStorageTimeZone,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            boolean splitCacheable,
            List<HivePageSourceProvider.ColumnMapping> columnMappings,
            Map<Integer, HiveCoercer> coercers);
}
