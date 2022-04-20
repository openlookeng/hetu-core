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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * redis.properties be mapped to this config .
 */
public class RedisConfig
{
    /**
     * redis default port .
     */
    private static final int REDIS_DEFAULT_PORT = 6379;
    /**
     * if your redis server have been protected by password,you should set .
     */
    private String redisPassword;

    /**
     * Timeout to connect to Redis .
     */
    private Duration redisConnectTimeout = new Duration(2000, MILLISECONDS);

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given table.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Redis values.
     */
    private File tableDescriptionDir = new File("etc/redis/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Whether Redis key string follows "schema:table:*" format .
     */
    private boolean keyPrefixSchemaTable;
    /**
     * Seed nodes for Redis cluster .
     */
    private Set<HostAddress> nodes = ImmutableSet.of();
    /**
     * SCAN cursor [MATCH pattern] [COUNT count]
     * this is Count parameter .
     */
    private int redisScanCount = 100;
    /**
     *  redis database index default 0 .
     */
    private int redisDataBaseIndex;
    /**
     * delimiter for separating schema name and table name in the KEY prefix .
     */
    private char redisKeyDelimiter = ':';
    /**
     * the interval of flush the the interval of flush description files (ms) (default no flush,table description will be memoized without expiration) .
     */
    private long tableDescriptionFlushInterval;

    @Config("redis.table-description-interval")
    public RedisConfig setTableDescriptionFlushInterval(final long tableDescriptionFlushInterval)
    {
        this.tableDescriptionFlushInterval = tableDescriptionFlushInterval;
        return this;
    }

    @Config("redis.table-description-dir")
    public RedisConfig setTableDescriptionDir(final File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @Config("redis.table-names")
    public RedisConfig setTableNames(final String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @Config("redis.default-schema")
    public RedisConfig setDefaultSchema(final String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Config("redis.nodes")
    public RedisConfig setNodes(final String nodes)
    {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        Spliterator<String> spliterator = splitter.split(nodes).spliterator();
        ImmutableSet<HostAddress> collect = StreamSupport.stream(spliterator, false).map((value) -> {
            return HostAddress.fromString(value).withDefaultPort(REDIS_DEFAULT_PORT);
        }).collect(toImmutableSet());
        this.nodes = (nodes == null) ? null : collect;
        return this;
    }

    @Config("redis.scan-count")
    public RedisConfig setRedisScanCount(final int redisScanCount)
    {
        this.redisScanCount = redisScanCount;
        return this;
    }

    @Config("redis.key-delimiter")
    public RedisConfig setRedisKeyDelimiter(final String redisKeyDelimiter)
    {
        this.redisKeyDelimiter = redisKeyDelimiter.charAt(0);
        return this;
    }

    @Config("redis.database-index")
    public RedisConfig setRedisDataBaseIndex(final int redisDataBaseIndex)
    {
        this.redisDataBaseIndex = redisDataBaseIndex;
        return this;
    }

    @Config("redis.password")
    public RedisConfig setRedisPassword(final String redisPassword)
    {
        this.redisPassword = redisPassword;
        return this;
    }

    @Config("redis.connect-timeout")
    public RedisConfig setRedisConnectTimeout(final String redisConnectTimeout)
    {
        this.redisConnectTimeout = Duration.valueOf(redisConnectTimeout);
        return this;
    }

    @Config("redis.hide-internal-columns")
    public RedisConfig setHideInternalColumns(final boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    @Config("redis.key-prefix-schema-table")
    public RedisConfig setKeyPrefixSchemaTable(final boolean keyPrefixSchemaTable)
    {
        this.keyPrefixSchemaTable = keyPrefixSchemaTable;
        return this;
    }

    public long getTableDescriptionFlushInterval()
    {
        return tableDescriptionFlushInterval;
    }

    public String getRedisPassword()
    {
        return redisPassword;
    }

    public Duration getRedisConnectTimeout()
    {
        return redisConnectTimeout;
    }

    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    public Set<String> getTableNames()
    {
        return tableNames;
    }

    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    public boolean isKeyPrefixSchemaTable()
    {
        return keyPrefixSchemaTable;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    public int getRedisScanCount()
    {
        return redisScanCount;
    }

    public int getRedisDataBaseIndex()
    {
        return redisDataBaseIndex;
    }

    public char getRedisKeyDelimiter()
    {
        return redisKeyDelimiter;
    }
}
