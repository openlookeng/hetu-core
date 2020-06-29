/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.heuristicindex;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.hetu.core.spi.heuristicindex.DataSource;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * The IndexerFactory is used to load the provided plugins (SPI implementations)
 * and use the provided configurations to configure these implementations.
 * It will return configured IndexClient and IndexWriter objects to be used for
 * reading, writing and deleting indexes.
 */
public class IndexFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(IndexFactory.class);

    private static final String CONFIG_FILE = "config.properties";
    private static final String CATALOG_CONFIGS_DIR = "catalog";
    private static final String INDEXSTORE_TYPE_KEY = "type";
    private static final String DATASTORE_TYPE_KEY = "connector.name";
    private static final String INDEXSTORE_KEYS_PREFIX = "hetu.filter.indexstore.";
    private static final String INDEX_KEYS_PREFIX = "hetu.filter.index.";

    private static IndexFactory instance;

    /**
     * Helper class for binding necessary classes for the Indexer
     *
     * @since 2019-11-27
     */
    private static class IndexModule
            extends AbstractModule
    {
        @Override
        protected void configure()
        {
            // bind PluginManager and load plugins
            bind(PluginManager.class).in(Scopes.SINGLETON);
        }
    }

    private Injector injector;

    private IndexFactory()
    {
        injector = Guice.createInjector(new IndexModule());

        getPluginManager().loadNativePlugins();
    }

    /**
     * Singleton constructor of IndexFactory
     *
     * @return Singleton instance of IndexFactory
     */
    public static synchronized IndexFactory getInstance()
    {
        if (instance == null) {
            instance = new IndexFactory();
        }

        return instance;
    }

    /**
     * Loads the specified plugins which include implementations of SPIs from
     * io.prestosql.spi.index.
     * <p>
     * Note: do not use jar-with-dependencies, instead use a directory with all the
     * plugin dependencies and the SPI implementation.
     *
     * @param plugins Absolute paths to the plugin(s), if path is a dir, all plugins
     *                are loaded recursively.
     * @return An instance of IndexFactory with the given plugins loaded to the classpath
     * @throws IOException Thrown when loading the plugins from files
     */
    public IndexFactory loadPlugins(String[] plugins) throws IOException
    {
        getPluginManager().loadPlugins(plugins);
        return this;
    }

    /**
     * Gets instance of the PluginManager
     *
     * @return Returns the plugin manager instance
     */
    protected PluginManager getPluginManager()
    {
        return injector.getInstance(PluginManager.class);
    }

    /**
     * Creates an IndexWriter configured with DataSource, IndexStore and Index(es)
     * using the provided properties
     * <br>
     * The DataSource will be selected based on the "connector.name "property in dataSourceProps.
     * The remaining properties in dataSourceProps will be used to configure the DataSource.
     * <br>
     * The IndexStore will be selected based on the "type" property in indexStoreProps.
     * The remaining properties in indexStoreProps will be used to configure the IndexStore.
     * <br>
     * The Index(es) will be selected based on the loaded Index implementations.
     * Each Index type will be configured based on the provided indexProps.
     * The prefix of the property in indexProps will be the ID of the corresponding Index type.
     * For example, properties prefixed with "bloom" will be applied to the BloomIndex
     *
     * @param dataSourceProps DataSource config properties
     * @param indexStoreProps IndexStore config properties
     * @param indexProps      Index config properties
     * @return Returns the IndexWriter loaded with the corresponding DataSource and IndexStore objects as given in the
     * config files
     */
    public IndexWriter getIndexWriter(Properties dataSourceProps, Properties indexStoreProps,
                                      Properties indexProps)
    {
        requireNonNull(dataSourceProps, "No datasource properties specified");
        requireNonNull(indexStoreProps, "No indexstore properties specified");
        requireNonNull(indexProps, "No index properties specified");

        LOG.debug("dataSourceProps: {}", dataSourceProps);
        LOG.debug("indexStoreProps: {}", indexStoreProps);
        LOG.debug("indexProps: {}", indexProps);

        PluginManager pluginManager = getPluginManager();

        // bind correct DataSource, Index and IndexStore based on input data
        Injector dynamicInjector = injector.createChildInjector(new AbstractModule()
        {
            @Override
            public void configure()
            {
                // DataSource
                DataSource dataSource = loadDataSource(pluginManager, dataSourceProps);
                LOG.debug("Using DataSource: {}", dataSource);
                bind(DataSource.class).toInstance(dataSource);

                // IndexStore
                IndexStore indexStore = loadIndexStore(pluginManager, indexStoreProps);
                LOG.debug("using IndexStore: {}", indexStore);
                bind(IndexStore.class).toInstance(indexStore);

                // Index types
                Multibinder<Index> multibinder = Multibinder.newSetBinder(binder(), Index.class);

                // set all supported index types
                pluginManager.getIndexes().forEach(e -> {
                    LOG.debug("Using Index: {}", e);
                    // set the properties and bind the object
                    e.setProperties(getPropertiesSubset(indexProps, e.getId().toLowerCase(Locale.ENGLISH)));
                    multibinder.addBinding().to(e.getClass());
                });
            }
        });

        IndexWriter indexWriter = dynamicInjector.getInstance(IndexWriter.class);
        return indexWriter;
    }

    /**
     * Creates an IndexWriter configured with DataSource, IndexStore and Index(es),
     * using the properties read from the config dir
     * <p>
     * See {@link #getIndexWriter(Properties, Properties, Properties)}
     * for details.
     *
     * @param table         Fully qualified table name used to search for the catalog file, i.e. catalog.schema.table
     * @param configRootDir Hetu config dir
     * @return Returns the IndexWriter loaded with the corresponding DataSource and IndexStore objects as given in the
     * configuration directory
     * @throws IOException Thrown from loading properties in the configuration directory
     */
    public IndexWriter getIndexWriter(String table, String configRootDir) throws IOException
    {
        requireNonNull(configRootDir, "No config dir specified");

        Properties dataSourceProps = loadDataSourceProperties(table, configRootDir);
        Properties indexStoreProps = loadIndexStoreProperties(configRootDir);
        Properties indexProps = loadIndexProperties(configRootDir);

        return getIndexWriter(dataSourceProps, indexStoreProps, indexProps);
    }

    /**
     * Creates an IndexClient configured with the IndexStore using the provided
     * properties.
     * <br>
     * The IndexStore will be selected based on the "type" property in indexStoreProps.
     * The remaining properties in indexStoreProps will be used to configure the IndexStore.
     *
     * @param indexStoreProps indexstore properties
     * @return An IndexClient which has the IndexStore configured
     */
    public IndexClient getIndexClient(Properties indexStoreProps)
    {
        requireNonNull(indexStoreProps, "No indexstore properties specified");

        LOG.debug("indexStoreProps: {}", indexStoreProps);

        PluginManager pluginManager = getPluginManager();

        // bind correct IndexStore based on config
        Injector dynamicInjector = injector.createChildInjector(new AbstractModule()
        {
            @Override
            public void configure()
            {
                // IndexStore
                IndexStore indexStore = loadIndexStore(pluginManager, indexStoreProps);
                LOG.debug("using IndexStore: {}", indexStore);
                bind(IndexStore.class).toInstance(indexStore);

                // Index types
                Multibinder<Index> multibinder = Multibinder.newSetBinder(binder(), Index.class);
                // bind all supported index types
                pluginManager.getIndexes().forEach(e -> {
                    LOG.debug("using Index: {}", e);
                    multibinder.addBinding().to(e.getClass());
                });
            }
        });

        return dynamicInjector.getInstance(IndexClient.class);
    }

    /**
     * Reads the indexstore properties from the config dir
     * <br>
     * See {@link #getIndexClient(Properties)} for details.
     *
     * @param configRootDir absolute path to the config dir
     * @return an IndexClient which has the IndexStore configured
     * @throws IOException Thrown from loading the configuration files
     */
    public IndexClient getIndexClient(String configRootDir) throws IOException
    {
        requireNonNull(configRootDir, "no config dir specified");

        Properties indexStoreProps = loadIndexStoreProperties(configRootDir);

        return getIndexClient(indexStoreProps);
    }

    /**
     * Loads the properties from config.properties in configRootDir and reads
     * only the indexstore properties
     *
     * @param configRootDir The path to the directory that contains the configuration files
     * @return Properties object with the corresponding IndexStore properties loaded from the configuration files
     * @throws IOException Thrown by reading the configuration files
     */
    protected static Properties loadIndexStoreProperties(String configRootDir) throws IOException
    {
        // load all properties from config.properties
        File configPropertiesFile = Paths.get(configRootDir, CONFIG_FILE).toFile();
        Properties properties = IndexServiceUtils.loadProperties(configPropertiesFile);

        return getPropertiesSubset(properties, INDEXSTORE_KEYS_PREFIX);
    }

    /**
     * Loads the properties from config.properties in configRootDir and reads
     * only the index properties
     *
     * @param configRootDir The path to the directory that contains the configuration files
     * @return Properties object with global indexer properties loaded from the configuration files
     * @throws IOException Thrown by reading the configuration files
     */
    protected static Properties loadIndexProperties(String configRootDir) throws IOException
    {
        // load all properties from config.properties
        File configPropertiesFile = Paths.get(configRootDir, CONFIG_FILE).toFile();
        Properties properties = IndexServiceUtils.loadProperties(configPropertiesFile);

        return getPropertiesSubset(properties, INDEX_KEYS_PREFIX);
    }

    /**
     * Loads the properties from the corresponding catalog properties file
     *
     * @param fullyQualifiedTableName Fully qualified table name used to search for the catalog file,
     *                                i.e. catalog.schema.table
     * @param configRootDir           The path to the directory that contains the configuration files
     * @return Properties object with the corresponding DataSource properties loaded from the configuration files
     * @throws IOException Thrown by reading the configuration files
     */
    protected static Properties loadDataSourceProperties(String fullyQualifiedTableName,
                                                         String configRootDir) throws IOException
    {
        String[] parts = IndexServiceUtils.getTableParts(fullyQualifiedTableName);
        String catalog = parts[0];

        // load the catalog properties in catalog dir
        File catalogPropertiesFile = Paths.get(configRootDir, CATALOG_CONFIGS_DIR, catalog + ".properties").toFile();

        return IndexServiceUtils.loadProperties(catalogPropertiesFile);
    }

    /**
     * Returns the subset of properties that start with the prefix, prefix is removed
     *
     * @param properties <code>Properties</code> object to extract properties with
     * @param prefix     A String prefix of some property key
     * @return A new <code>Properties</code> object with all property key starting with the <code>prefix</code>
     * given and is present in <code>properties</code>,
     * and the value of each key is exactly the same as it is in <code>properties</code>
     */
    private static Properties getPropertiesSubset(Properties properties, String prefix)
    {
        Properties subsetProps = new Properties();
        properties.stringPropertyNames().forEach(k -> {
            if (k.startsWith(prefix)) {
                subsetProps.put(k.substring(k.indexOf(prefix) + prefix.length()), properties.getProperty(k));
            }
        });

        return subsetProps;
    }

    private DataSource loadDataSource(PluginManager pluginManager, Properties dataSourceProps)
    {
        String dataSourceName = dataSourceProps.getProperty(DATASTORE_TYPE_KEY);
        requireNonNull(dataSourceName, "No datasource found");
        DataSource dataSource = pluginManager.getDataSource(dataSourceName);

        if (dataSource == null) {
            LOG.error("DataSource not supported: {}", dataSourceName);
            throw new IllegalArgumentException("DataSource not supported: " + dataSourceName);
        }

        // set the properties and bind the object
        dataSource.setProperties(dataSourceProps);

        return dataSource;
    }

    private IndexStore loadIndexStore(PluginManager pluginManager, Properties indexStoreProps)
    {
        String indexStoreName = indexStoreProps.getProperty(INDEXSTORE_TYPE_KEY);
        requireNonNull(indexStoreName, "No index store found");
        IndexStore indexStore = pluginManager.getIndexStore(indexStoreName);

        if (indexStore == null) {
            LOG.error("IndexStore not supported: {}", indexStoreName);
            throw new IllegalArgumentException("IndexStore not supported: " + indexStoreName);
        }

        // set the properties and bind the object
        indexStore.setProperties(indexStoreProps);

        return indexStore;
    }
}
