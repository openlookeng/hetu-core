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
package io.prestosql.plugin.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.NodeManager;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on the segments and partitions
 * it needs to process. According to the Kafka source code, a Kafka {@link kafka.javaapi.consumer.SimpleConsumer} is thread-safe.
 */
public class KafkaSimpleConsumerManager
{
    private static final Logger log = Logger.get(KafkaSimpleConsumerManager.class);

    private final LoadingCache<HostAddress, SimpleConsumer> consumerCache;

    private final NodeManager nodeManager;
    private final int connectTimeoutMillis;
    private final int bufferSizeBytes;

    private final boolean kerberosOn;
    private final String loginConfig;
    private final String krb5Conf;
    private String groupId;
    private final String securityProtocol;
    private final String saslMechanism;
    private final String saslKerberosServiceName;

    @Inject
    public KafkaSimpleConsumerManager(
            KafkaConnectorConfig kafkaConnectorConfig,
            NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.connectTimeoutMillis = toIntExact(kafkaConnectorConfig.getKafkaConnectTimeout().toMillis());
        this.bufferSizeBytes = toIntExact(kafkaConnectorConfig.getKafkaBufferSize().toBytes());

        this.consumerCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::createConsumer));

        this.kerberosOn = kafkaConnectorConfig.isKerberosOn();
        this.loginConfig = kafkaConnectorConfig.getLoginConfig();
        this.krb5Conf = kafkaConnectorConfig.getKrb5Conf();
        this.groupId = kafkaConnectorConfig.getGroupId();
        this.securityProtocol = kafkaConnectorConfig.getSecurityProtocol();
        this.saslMechanism = kafkaConnectorConfig.getSaslMechanism();
        this.saslKerberosServiceName = kafkaConnectorConfig.getSaslKerberosServiceName();
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<HostAddress, SimpleConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                log.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public SimpleConsumer getConsumer(HostAddress host)
    {
        requireNonNull(host, "host is null");
        return consumerCache.getUnchecked(host);
    }

    public KafkaConsumer<ByteBuffer, ByteBuffer> getSaslConsumer(HostAddress host)
    {
        requireNonNull(host, "host is null");
        return createSaslConsumer(host);
    }

    private SimpleConsumer createConsumer(HostAddress host)
    {
        log.info("Creating new Consumer for %s", host);
        return new SimpleConsumer(host.getHostText(),
                host.getPort(),
                connectTimeoutMillis,
                bufferSizeBytes,
                "presto-kafka-" + nodeManager.getCurrentNode().getNodeIdentifier());
    }

    private KafkaConsumer<ByteBuffer, ByteBuffer> createSaslConsumer(HostAddress host)
    {
        log.info("Creating new SaslConsumer for %s", host);
        Properties props = new Properties();
        if (kerberosOn) {
            System.setProperty("java.security.auth.login.config", loginConfig);
            System.setProperty("java.security.krb5.conf", krb5Conf);
            props.put("security.protocol", securityProtocol);
            props.put("sasl.mechanism", saslMechanism);
            props.put("sasl.kerberos.service.name", saslKerberosServiceName);
        }

        try {
            props.put("bootstrap.servers", host.toString());
            props.put("enable.auto.commit", "false");
            props.put("key.deserializer", Class.forName("org.apache.kafka.common.serialization.ByteBufferDeserializer"));
            props.put("value.deserializer", Class.forName("org.apache.kafka.common.serialization.ByteBufferDeserializer"));
            if (groupId == null) {
                groupId = UUID.randomUUID().toString();
            }
            props.put("group.id", groupId);
            props.put("session.timeout.ms", connectTimeoutMillis);
            props.put("receive.buffer.bytes", bufferSizeBytes);
        }
        catch (ClassNotFoundException e) {
            log.error(e, "failed to create kafka consumer");
        }

        return new KafkaConsumer<>(props);
    }
}
