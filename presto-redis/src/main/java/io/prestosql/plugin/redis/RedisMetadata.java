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
package io.prestosql.plugin.redis;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.plugin.redis.description.RedisInternalFieldDescription;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.plugin.redis.description.RedisTableFieldDescription;
import io.prestosql.plugin.redis.description.RedisTableFieldGroup;
import io.prestosql.plugin.redis.handle.RedisColumnHandle;
import io.prestosql.plugin.redis.handle.RedisTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class RedisMetadata
        implements ConnectorMetadata
{
    private static final Logger LOG = Logger.get(RedisMetadata.class);
    private final RedisConfig redisConfig;
    private final boolean hideInternalColumns;
    private final Supplier<Map<SchemaTableName, RedisTableDescription>> mapRedisTableDescription;

    @Inject
    public RedisMetadata(
            final RedisConfig redisConfig,
            final Supplier<Map<SchemaTableName, RedisTableDescription>> redisTableDescriptionSupplier)
    {
        this.redisConfig = requireNonNull(redisConfig, "client is null");
        hideInternalColumns = redisConfig.isHideInternalColumns();
        LOG.debug("Loading redis table definitions from %s", redisConfig.getTableDescriptionDir().getAbsolutePath());
        long tableDescriptionFlushInterval = redisConfig.getTableDescriptionFlushInterval();
        if (tableDescriptionFlushInterval <= 0) {
            this.mapRedisTableDescription = Suppliers.memoize(redisTableDescriptionSupplier::get)::get;
        }
        else {
            this.mapRedisTableDescription = Suppliers.memoizeWithExpiration(redisTableDescriptionSupplier::get, tableDescriptionFlushInterval, TimeUnit.MILLISECONDS)::get;
        }
    }

    @Override
    public List<String> listSchemaNames(final ConnectorSession session)
    {
        return mapRedisTableDescription.get().keySet().stream().map(SchemaTableName::getSchemaName).distinct().collect(Collectors.toList());
    }

    /**
     * if schemaName is null return all SchemaTableName else return matched SchemaTableName
     * @param session
     * @param schemaName
     * @return List
     *
     */
    @Override
    public List<SchemaTableName> listTables(final ConnectorSession session, final Optional<String> schemaName)
    {
        return mapRedisTableDescription.get().keySet().stream().filter(schemaTableName -> {
            return !schemaName.isPresent() || schemaTableName.getSchemaName().equals(schemaName.get());
        }).collect(Collectors.toList());
    }

    @Nullable
    public RedisTableHandle getTableHandle(final ConnectorSession session, final SchemaTableName schemaTableName)
    {
        RedisTableDescription redisTableDescription = this.mapRedisTableDescription.get().get(schemaTableName);
        if (redisTableDescription == null) {
            return null;
        }
        String keyName = null;
        if (redisTableDescription.getKey() != null) {
            if (redisTableDescription.getKey().getName().isPresent()) {
                keyName = redisTableDescription.getKey().getName().get();
            }
        }
        RedisTableFieldGroup key = redisTableDescription.getKey();
        RedisTableFieldGroup value = redisTableDescription.getValue();
        String keyDataformat = (key == null) ? DummyRowDecoder.NAME : key.getDataFormat();
        String valueDataformat = (value == null) ? DummyRowDecoder.NAME : value.getDataFormat();

        return new RedisTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                keyDataformat,
                valueDataformat,
                keyName);
    }

    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session, final ConnectorTableHandle tableHandle)
    {
        SchemaTableName schemaTableName = ((RedisTableHandle) tableHandle).toSchemaTableName();
        ConnectorTableMetadata tableMetadata = getTableMetadata(schemaTableName);
        if (tableMetadata == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return tableMetadata;
    }

    @Nullable
    public ConnectorTableMetadata getTableMetadata(final SchemaTableName schemaTableName)
    {
        RedisTableDescription table = this.mapRedisTableDescription.get().get(schemaTableName);
        if (table == null) {
            return null;
        }
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        if (table.getKey() != null) {
            List<RedisTableFieldDescription> fields = table.getKey().getFields();
            if (fields != null) {
                fields.forEach(x -> {
                    builder.add(x.getColumnMetadata());
                });
            }
        }
        if (table.getValue() != null) {
            List<RedisTableFieldDescription> fields = table.getValue().getFields();
            if (fields != null) {
                fields.forEach(x -> {
                    builder.add(x.getColumnMetadata());
                });
            }
        }
        for (RedisInternalFieldDescription value : RedisInternalFieldDescription.values()) {
            builder.add(value.getColumnMetadata(hideInternalColumns));
        }
        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session, final ConnectorTableHandle tableHandle)
    {
        RedisTableHandle redisTableHandle = ((RedisTableHandle) tableHandle);
        RedisTableDescription redisTableDescription = mapRedisTableDescription.get().get(redisTableHandle.toSchemaTableName());
        if (redisTableDescription == null) {
            throw new TableNotFoundException(redisTableHandle.toSchemaTableName());
        }
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        RedisTableFieldGroup key = redisTableDescription.getKey();
        AtomicInteger index = new AtomicInteger();
        if (key != null) {
            List<RedisTableFieldDescription> fields = key.getFields();
            if (fields != null) {
                fields.forEach(field -> {
                    builder.put(field.getName(), field.getColumnHandle(true, index.getAndIncrement()));
                });
            }
        }
        RedisTableFieldGroup value = redisTableDescription.getValue();
        if (value != null) {
            List<RedisTableFieldDescription> fields = value.getFields();
            if (fields != null) {
                fields.forEach(field -> {
                    builder.put(field.getName(), field.getColumnHandle(false, index.getAndIncrement()));
                });
            }
        }
        for (RedisInternalFieldDescription field : RedisInternalFieldDescription.values()) {
            builder.put(field.getColumnName(), field.getColumnHandle(index.getAndIncrement(), hideInternalColumns));
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(final ConnectorSession session, final SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        List<SchemaTableName> tableNames = prefix.getTable().isPresent()
                ? ImmutableList.of(prefix.toSchemaTableName())
                : listTables(session, prefix.getSchema());
        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                builder.put(tableName, tableMetadata.getColumns());
            }
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(final ConnectorSession session, final ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((RedisColumnHandle) columnHandle).getColumnMetadata();
    }

    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(final ConnectorSession session, final ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }
}
