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
package io.prestosql.plugin.redis.description;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.plugin.redis.RedisConfig;
import io.prestosql.spi.connector.SchemaTableName;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public class RedisTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, RedisTableDescription>>
{
    private static final Logger LOG = Logger.get(RedisTableDescriptionSupplier.class);
    private final RedisConfig redisConfig;
    private final JsonCodec<RedisTableDescription> tableDescriptionJsonCodec;

    @Inject
    public RedisTableDescriptionSupplier(
            final RedisConfig redisConfig,
            final JsonCodec<RedisTableDescription> tableDescriptionJsonCodec)
    {
        this.redisConfig = requireNonNull(redisConfig, "redisConnectorConfig is null");
        this.tableDescriptionJsonCodec = requireNonNull(tableDescriptionJsonCodec, "tableDescriptionCodec is null");
    }

    @Override
    public Map<SchemaTableName, RedisTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> builder = ImmutableMap.builder();
        try {
            for (File file : requireNonNull(redisConfig.getTableDescriptionDir().listFiles())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    RedisTableDescription redisTableDescription = tableDescriptionJsonCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName;
                    if (redisTableDescription.getSchemaName() != null) {
                        schemaName = redisTableDescription.getSchemaName();
                    }
                    else if (redisConfig.getDefaultSchema() != null) {
                        schemaName = redisConfig.getDefaultSchema();
                    }
                    else {
                        throw new NullPointerException("Both parameters are null");
                    }
                    LOG.debug("Redis table %s Description %s", redisTableDescription.getTableName(), redisTableDescription);
                    builder.put(new SchemaTableName(schemaName, redisTableDescription.getTableName()), redisTableDescription);
                }
            }
            ImmutableMap<SchemaTableName, RedisTableDescription> tableDefinitions = builder.build();
            LOG.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : redisConfig.getTableNames()) {
                SchemaTableName tableName;
                if (definedTable.contains(".")) {
                    List<String> schandtable = Splitter.on('.').splitToList(definedTable);
                    checkArgument(schandtable.size() == 2, "Invalid schemaTableName: %s", schandtable);
                    tableName = new SchemaTableName(schandtable.get(0), schandtable.get(1));
                }
                else {
                    LOG.debug("you don't set table schema , now you table schema is DefaultSchema");
                    tableName = new SchemaTableName(redisConfig.getDefaultSchema(), definedTable);
                }
                if (tableDefinitions.containsKey(tableName)) {
                    RedisTableDescription redisTable = tableDefinitions.get(tableName);
                    LOG.debug("Found Table definition for %s: %s", tableName, redisTable);
                    builder.put(tableName, redisTable);
                }
                else {
                    // A dummy table definition only supports the internal columns.
                    LOG.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new RedisTableDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of()),
                            new RedisTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of())));
                }
            }
            return builder.build();
        }
        catch (IOException e) {
            LOG.error("Failed to load table description,please check table description files");
            throw new UncheckedIOException(e);
        }
    }
}
