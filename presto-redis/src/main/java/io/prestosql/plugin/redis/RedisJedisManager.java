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

import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class RedisJedisManager
{
    private static final Logger LOG = Logger.get(RedisJedisManager.class);
    private final ConcurrentMap<HostAddress, JedisPool> jedisPoolConcurrentMap = new ConcurrentHashMap<>();

    private final RedisConfig redisConfig;
    private final JedisPoolConfig jedisPoolConfig;

    @Inject
    public RedisJedisManager(final RedisConfig redisConfig)
    {
        this.redisConfig = requireNonNull(redisConfig, "redisconfig is null");
        this.jedisPoolConfig = new JedisPoolConfig();
    }

    public JedisPool getJedisPool(final HostAddress host)
    {
        requireNonNull(host, "host is null");
        return jedisPoolConcurrentMap.computeIfAbsent(host, hostAddress -> {
            LOG.info("Creating new JedisPool for %s", hostAddress);
            return new JedisPool(jedisPoolConfig,
                    hostAddress.getHostText(),
                    hostAddress.getPort(),
                    toIntExact(redisConfig.getRedisConnectTimeout().toMillis()),
                    redisConfig.getRedisPassword(),
                    redisConfig.getRedisDataBaseIndex());
        });
    }

    public RedisConfig getRedisConfig()
    {
        return redisConfig;
    }

    @PreDestroy
    public void destoryall()
    {
        for (Map.Entry<HostAddress, JedisPool> hostAddressJedisPoolEntry : jedisPoolConcurrentMap.entrySet()) {
            try {
                hostAddressJedisPoolEntry.getValue().destroy();
            }
            catch (Exception e) {
                LOG.warn(e, "While destroying JedisPool %s:", hostAddressJedisPoolEntry.getKey());
            }
        }
    }
}
