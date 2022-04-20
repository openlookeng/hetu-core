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
package io.prestosql.plugin.redis.split;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.plugin.redis.RedisConfig;
import io.prestosql.plugin.redis.RedisJedisManager;
import io.prestosql.plugin.redis.handle.RedisTableHandle;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RedisSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(RedisSplitManager.class);
    private final RedisConfig redisConfig;
    private final RedisJedisManager jedisManager;
    private static final long REDIS_MAX_SPLITS = 100;
    private static final long REDIS_STRIDE_SPLITS = 100;

    @Inject
    public RedisSplitManager(final RedisConfig redisConfig, final RedisJedisManager jedisManager)
    {
        this.redisConfig = requireNonNull(redisConfig, "client is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            final ConnectorTransactionHandle transaction,
            final ConnectorSession session,
            final ConnectorTableHandle connectorTableHandle,
            final SplitSchedulingStrategy splitSchedulingStrategy)
    {
        RedisTableHandle tableHandle = (RedisTableHandle) connectorTableHandle;
        List<HostAddress> nodes = new ArrayList<>(redisConfig.getNodes());
        Collections.shuffle(nodes);
        checkState(!nodes.isEmpty(), "No Redis nodes available");
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        long numberOfKeys = 1;
        if (tableHandle.getKeyDataFormat().equals("zset")) {
            try {
                Jedis resource = jedisManager.getJedisPool(nodes.get(0)).getResource();
                numberOfKeys = resource.zcount(tableHandle.getKeyName(), "-inf", "+inf");
                resource.close();
            }
            catch (Exception e) {
                LOG.error("key dataformat is zset,failed to zcount from redis server");
            }
        }
        long stride = REDIS_STRIDE_SPLITS;
        if (numberOfKeys / stride > REDIS_MAX_SPLITS) {
            stride = numberOfKeys / REDIS_MAX_SPLITS;
        }
        for (long startIndex = 0; startIndex < numberOfKeys; startIndex += stride) {
            long endIndex = startIndex + stride - 1;
            if (endIndex >= numberOfKeys) {
                endIndex = -1;
            }
            RedisSplit split = new RedisSplit(
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    tableHandle.getKeyDataFormat(),
                    tableHandle.getValueDataFormat(),
                    tableHandle.getKeyName(),
                    startIndex,
                    endIndex,
                    nodes);

            builder.add(split);
        }
        return new FixedSplitSource(builder.build());
    }
}
