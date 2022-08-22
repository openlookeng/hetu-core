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
package io.prestosql.exchange;

import io.airlift.log.Logger;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.exchange.ExchangeHandleResolver;
import io.prestosql.spi.exchange.ExchangeManager;
import io.prestosql.spi.exchange.ExchangeManagerFactory;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExchangeManagerRegistry
{
    private static final Logger log = Logger.get(ExchangeManagerRegistry.class);

    private static final File CONFIG_FILE = new File("etc/exchange-manager.properties");
    private static final String EXCHANGE_MANAGER_NAME_PROPERTY = "exchange-manager.name";

    private final ExchangeHandleResolver handleResolver;

    private final Map<String, ExchangeManagerFactory> exchangeManagerFactories = new ConcurrentHashMap<>();

    private volatile ExchangeManager exchangeManager;

    @Inject
    public ExchangeManagerRegistry(ExchangeHandleResolver handleResolver)
    {
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
    }

    public void addExchangeManagerFactory(ExchangeManagerFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (exchangeManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Exchange manager factory '%s' is already registered", factory.getName()));
        }
    }

    public void loadExchangeManager(FileSystemClientManager fileSystemClientManager)
    {
        if (!CONFIG_FILE.exists()) {
            return;
        }

        Map<String, String> properties = loadProperties(CONFIG_FILE);
        String name = properties.remove(EXCHANGE_MANAGER_NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name), "Exchange manager configuration %s does not contain %s property", CONFIG_FILE, EXCHANGE_MANAGER_NAME_PROPERTY);

        String filesystemType = properties.get("exchange-filesystem-type");
        String filesystemBaseDirectory = properties.get("exchange.base-directories");
        HetuFileSystemClient fileSystemClient;
        try {
            fileSystemClient = fileSystemClientManager.getFileSystemClient(filesystemType, Paths.get(filesystemBaseDirectory));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        loadExchangeManager(name, properties, fileSystemClient);
    }

    public synchronized void loadExchangeManager(String name, Map<String, String> properties, HetuFileSystemClient fileSystemClient)
    {
        log.info("-- Loading exchange manager %s --", name);

        checkState(exchangeManager == null, "exchangeManager is already loaded");

        ExchangeManagerFactory factory = exchangeManagerFactories.get(name);
        checkArgument(factory != null, "Exchange manager factory '%s' is not registered. Available factories: %s", name, exchangeManagerFactories.keySet());

        ExchangeManager exchangeManagerInstance;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            exchangeManagerInstance = factory.create(properties, fileSystemClient);
        }
        handleResolver.setExchangeManagerHandleResolver(factory.getHandleResolver());

        log.info("-- Loaded exchange manager %s --", name);

        this.exchangeManager = exchangeManagerInstance;
    }

    public ExchangeManager getExchangeManager()
    {
        ExchangeManager exchangeManagerInstance = this.exchangeManager;
        if (exchangeManagerInstance == null) {
            throw new RuntimeException("Exchange manager must be configured for the failure recovery capabilities to be fully functional");
        }
        return exchangeManagerInstance;
    }

    private static Map<String, String> loadProperties(File configFile)
    {
        try {
            return new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }
    }
}
