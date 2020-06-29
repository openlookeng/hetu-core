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

import com.google.common.collect.ImmutableList;
import io.hetu.core.spi.heuristicindex.DataSource;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Contains logic for dynamically loading the plugins to the classpath.
 */
public class PluginManager
{
    private static final Logger LOG = LoggerFactory.getLogger(PluginManager.class);

    private final ConcurrentMap<String, DataSource> dataSources = new ConcurrentHashMap<>(1);
    private final ConcurrentMap<String, IndexStore> indexStores = new ConcurrentHashMap<>(1);
    private final ConcurrentMap<String, Index> indexes = new ConcurrentHashMap<>(1);

    /**
     * Loads SPI implementations from the specified plugins.
     *
     * @param plugins path to the plugin(s), if dir, ll plugins inside are loaded
     *                recursively
     * @throws IOException thrown traversing through the file system
     */
    public void loadPlugins(String[] plugins) throws IOException
    {
        LOG.debug("loading plugins...");

        ImmutableList.Builder<URL> urlListBuilder = ImmutableList.builder();

        if (plugins == null) {
            return;
        }

        for (String plugin : plugins) {
            File file = new File(plugin);

            Files.walk(file.toPath())
                    .filter(Files::isRegularFile)
                    .forEach(p -> {
                        try {
                            urlListBuilder.add(p.toUri().toURL());
                        }
                        catch (MalformedURLException e) {
                            LOG.warn("Error loading plugin.", e);
                            LOG.warn("Without plugins hetu will not use the heuristic indexer in queries");
                        }
                    });
        }
        List<URL> urls = urlListBuilder.build();

        loadExternalPlugin(urls.toArray(new URL[0]));
    }

    /**
     * Loads the SPI implementations found inside this package.
     */
    protected void loadNativePlugins()
    {
        LOG.debug("loading plugins...");

        ServiceLoader<DataSource> dataSourceServiceLoader = ServiceLoader.load(DataSource.class);
        dataSourceServiceLoader.forEach(e -> {
            LOG.debug("loaded DataSource: {}", e);
            addDataSource(e);
        });

        ServiceLoader<Index> indexServiceLoader = ServiceLoader.load(Index.class);
        indexServiceLoader.forEach(e -> {
            LOG.debug("loaded Index: {}", e);
            addIndex(e);
        });

        ServiceLoader<IndexStore> indexStoreServiceLoader = ServiceLoader.load(IndexStore.class);
        indexStoreServiceLoader.forEach(e -> {
            LOG.debug("loaded IndexStore: {}", e);
            addIndexStore(e);
        });
    }

    private void loadExternalPlugin(URL[] urls)
    {
        URLClassLoader loader = URLClassLoader.newInstance(urls, getClass().getClassLoader());
        Thread.currentThread().setContextClassLoader(loader);

        ServiceLoader<DataSource> dataSourceServiceLoader = ServiceLoader.load(DataSource.class, loader);
        dataSourceServiceLoader.forEach(e -> {
            LOG.debug("loaded DataSource: {}", e);
            addDataSource(e);
        });

        ServiceLoader<Index> indexServiceLoader = ServiceLoader.load(Index.class, loader);
        indexServiceLoader.forEach(e -> {
            LOG.debug("loaded Index: {}", e);
            addIndex(e);
        });

        ServiceLoader<IndexStore> indexStoreServiceLoader = ServiceLoader.load(IndexStore.class, loader);
        indexStoreServiceLoader.forEach(e -> {
            LOG.debug("loaded IndexStore: {}", e);
            addIndexStore(e);
        });
    }

    /**
     * Simple get method
     *
     * @param name corresponding to datasource/catalog. Set in a .properties file in the catalogs folder
     * @return Datasource corresponding to the name.
     */
    public DataSource getDataSource(String name)
    {
        return dataSources.get(name.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Adds a Datasource/catalog
     *
     * @param dataSource datasource to be added
     */
    public void addDataSource(DataSource dataSource)
    {
        dataSources.put(dataSource.getId().toLowerCase(Locale.ENGLISH), dataSource);
    }

    /**
     * Simple get method
     *
     * @return all datasources that have been loaded in
     */
    public Collection<DataSource> getDataSources()
    {
        return dataSources.values();
    }

    /**
     * Simple get method
     *
     * @param name indexstore name
     * @return indexstore corresponding to the name
     */
    public IndexStore getIndexStore(String name)
    {
        return indexStores.get(name.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Simple add method
     *
     * @param indexStore indexstore to be added
     */
    public void addIndexStore(IndexStore indexStore)
    {
        indexStores.put(indexStore.getId().toLowerCase(Locale.ENGLISH), indexStore);
    }

    public Collection<IndexStore> getIndexStores()
    {
        return indexStores.values();
    }

    /**
     * Simple get method
     *
     * @param name of index to return
     * @return index instance whose ID equals name
     */
    public Index getIndex(String name)
    {
        return indexes.get(name.toLowerCase(Locale.ENGLISH));
    }

    /**
     * Simple add method
     *
     * @param index to be added
     */
    public void addIndex(Index index)
    {
        indexes.put(index.getId().toLowerCase(Locale.ENGLISH), index);
    }

    public Collection<Index> getIndexes()
    {
        return indexes.values();
    }
}
