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
package io.prestosql.plugin.redis.util;

import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;

public class RedisServer
{
    private static final int PORT = 6379;
    private final HostAndPort hostAndPort;
    private GenericContainer<?> container;
    private final JedisPool jedisPool;

    public RedisServer()
    {
        this.container = new GenericContainer<>("redis:2.8.9").withExposedPorts(PORT);
        container.start();
        this.jedisPool = new JedisPool(container.getContainerIpAddress(), container.getMappedPort(PORT));
        hostAndPort = new HostAndPort(container.getContainerIpAddress(), container.getMappedPort(PORT));
    }

    public RedisServer(HostAndPort hostAndPort)
    {
        this.hostAndPort = hostAndPort;
        this.jedisPool = new JedisPool(hostAndPort.getHost(), hostAndPort.getPort());
    }

    public JedisPool getJedisPool()
    {
        return jedisPool;
    }

    public void destoryJedisPool()
    {
        jedisPool.destroy();
    }

    public HostAndPort getHostAndPort()
    {
        return hostAndPort;
    }

    public void close()
    {
        jedisPool.close();
        if (container != null) {
            container.close();
        }
    }
}
