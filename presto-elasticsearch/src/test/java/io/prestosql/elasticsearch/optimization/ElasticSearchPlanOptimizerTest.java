package io.prestosql.elasticsearch.optimization;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.units.Duration;
import io.prestosql.elasticsearch.ElasticsearchClient;
import io.prestosql.elasticsearch.ElasticsearchConfig;
import io.prestosql.elasticsearch.ElasticsearchConnector;
import io.prestosql.elasticsearch.ElasticsearchMetadata;
import io.prestosql.elasticsearch.ElasticsearchPageSourceProvider;
import io.prestosql.elasticsearch.ElasticsearchSplitManager;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.type.InternalTypeManager;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.prestosql.elasticsearch.ElasticsearchConfig.Security.PASSWORD;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ElasticSearchPlanOptimizerTest
{
    static ElasticsearchConfig elasticsearchConfig;

    static ElasticSearchRowExpressionConverter elasticSearchRowExpressionConverter;

    static FunctionMetadataManager functionMetadataManager;

    static StandardFunctionResolution standardFunctionResolution;

    static ElasticSearchPlanOptimizer elasticSearchPlanOptimizer;

    static ElasticSearchPlanOptimizerProvider elasticSearchPlanOptimizerProvider;

    static ElasticsearchConnector elasticsearchConnector;

    static ElasticsearchMetadata elasticsearchMetadata;

    static ElasticsearchClient elasticsearchClient;

    static LifeCycleManager lifeCycleManager;

    static ElasticsearchSplitManager elasticsearchSplitManager;

    static ElasticsearchPageSourceProvider elasticsearchPageSourceProvider;

    static {
        // elastic search config for test
        elasticsearchConfig = new ElasticsearchConfig()
                .setHost("example.com")
                .setPort(9999)
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setConnectTimeout(new Duration(10, SECONDS))
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setNodeRefreshInterval(new Duration(10, MINUTES))
                .setMaxHttpConnections(100)
                .setHttpThreadCount(30)
                .setTlsEnabled(true)
                .setVerifyHostnames(false)
                .setSecurity(PASSWORD)
                .setPushDownEnabled(true);

        functionMetadataManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        standardFunctionResolution = new FunctionResolution(FunctionAndTypeManager.createTestFunctionAndTypeManager());
        elasticSearchRowExpressionConverter = new ElasticSearchRowExpressionConverter(functionMetadataManager, standardFunctionResolution);
        elasticSearchPlanOptimizer = new ElasticSearchPlanOptimizer(elasticsearchConfig, elasticSearchRowExpressionConverter);
        elasticSearchPlanOptimizerProvider = new ElasticSearchPlanOptimizerProvider(elasticSearchPlanOptimizer);

        elasticsearchClient = new ElasticsearchClient(elasticsearchConfig, Optional.empty());
        elasticsearchMetadata = new ElasticsearchMetadata(new InternalTypeManager(FunctionAndTypeManager.createTestFunctionAndTypeManager()), elasticsearchClient, elasticsearchConfig);

        lifeCycleManager = new LifeCycleManager(Collections.emptyList(), null);
        elasticsearchSplitManager = new ElasticsearchSplitManager(elasticsearchClient);
        elasticsearchPageSourceProvider = new ElasticsearchPageSourceProvider(elasticsearchClient);

        elasticsearchConnector = new ElasticsearchConnector(lifeCycleManager, elasticsearchMetadata, elasticsearchSplitManager, elasticsearchPageSourceProvider, elasticSearchPlanOptimizerProvider);
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("elasticsearch.host", "example.com")
                .put("elasticsearch.port", "9999")
                .put("elasticsearch.default-schema-name", "test")
                .put("elasticsearch.scroll-size", "4000")
                .put("elasticsearch.scroll-timeout", "20s")
                .put("elasticsearch.request-timeout", "1s")
                .put("elasticsearch.connect-timeout", "10s")
                .put("elasticsearch.max-retry-time", "10s")
                .put("elasticsearch.node-refresh-interval", "10m")
                .put("elasticsearch.max-http-connections", "100")
                .put("elasticsearch.http-thread-count", "30")
                .put("elasticsearch.tls.enabled", "true")
                .put("elasticsearch.tls.keystore-path", keystoreFile.toString())
                .put("elasticsearch.tls.keystore-password", "keystore-password")
                .put("elasticsearch.tls.truststore-path", truststoreFile.toString())
                .put("elasticsearch.tls.truststore-password", "truststore-password")
                .put("elasticsearch.tls.verify-hostnames", "false")
                .put("elasticsearch.security", "PASSWORD")
                .put("elasticsearch.pushdown.enabled", "true")
                .build();

        ElasticsearchConfig expected = new ElasticsearchConfig()
                .setHost("example.com")
                .setPort(9999)
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setConnectTimeout(new Duration(10, SECONDS))
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setNodeRefreshInterval(new Duration(10, MINUTES))
                .setMaxHttpConnections(100)
                .setHttpThreadCount(30)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTrustStorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setVerifyHostnames(false)
                .setSecurity(PASSWORD)
                .setPushDownEnabled(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testElasticsearchprovider()
    {
        Connector connector = elasticsearchConnector;
        ConnectorPlanOptimizerProvider connectorPlanOptimizerProvider = connector.getConnectorPlanOptimizerProvider();
        Set<ConnectorPlanOptimizer> logicalPlanOptimizers = connectorPlanOptimizerProvider.getLogicalPlanOptimizers();
        assertEquals(1, logicalPlanOptimizers.size());
        assertTrue(logicalPlanOptimizers.contains(elasticSearchPlanOptimizer));
        Set<ConnectorPlanOptimizer> physicalPlanOptimizers = connectorPlanOptimizerProvider.getPhysicalPlanOptimizers();
        assertEquals(0, physicalPlanOptimizers.size());
    }
}