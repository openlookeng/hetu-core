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
package io.prestosql.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import io.airlift.log.Logger;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.failuredetector.FailureRetryFactory;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FailureDetectorManager
{
    private static final Logger LOG = Logger.get(FailureDetectorManager.class);
    public static final String FD_RETRY_CONFIG_DIR = "etc/failure-retry-policy/";

    private final String failureRetryPolicyConfig;

    private static final List<FailureDetector> failureDetectors = new ArrayList<>();

    private static final Map<String, FailureRetryFactory> failureRetryFactories = new ConcurrentHashMap<>();
    private static final Map<String, Properties> availableFrConfigs = new ConcurrentHashMap<>();

    @VisibleForTesting
    public FailureDetectorManager(FailureDetector failureDetector, String maxErrorDuration)
    {
        requireNonNull(failureDetector, "failureDetector is null");
        failureDetectors.add(failureDetector);
        Properties defaultProfile = new Properties();
        defaultProfile.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, FailureRetryPolicy.TIMEOUT);
        defaultProfile.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, maxErrorDuration);
        availableFrConfigs.put(FailureRetryConfig.DEFAULT_CONFIG_NAME, defaultProfile);
        this.failureRetryPolicyConfig = FailureRetryConfig.DEFAULT_CONFIG_NAME;
    }

    @Inject
    public FailureDetectorManager(FailureRetryConfig cfg, FailureDetector failureDetector)
    {
        requireNonNull(failureDetector, "failureDetector is null");
        requireNonNull(cfg, "config is null");
        failureDetectors.add(failureDetector);
        Properties defaultProfile = new Properties();
        defaultProfile.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, FailureRetryPolicy.TIMEOUT);
        defaultProfile.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, FailureRetryPolicy.DEFAULT_TIMEOUT_DURATION);
        availableFrConfigs.putIfAbsent(cfg.getFailureRetryPolicyProfile(), defaultProfile);
        this.failureRetryPolicyConfig = cfg.getFailureRetryPolicyProfile();
    }

    @VisibleForTesting
    public static synchronized Map<String, Properties> getAvailableFrConfigs()
    {
        return availableFrConfigs;
    }

    public static synchronized FailureDetector getDefaultFailureDetector()
    {
        return failureDetectors.get(0);
    }

    public static synchronized void addFailureRetryFactory(FailureRetryFactory factory)
    {
        failureRetryFactories.putIfAbsent(factory.getName(), factory);
    }

    @VisibleForTesting
    public static synchronized Map<String, FailureRetryFactory> getFailureRetryFactories()
    {
        return failureRetryFactories;
    }

    @VisibleForTesting
    public static synchronized void removeallFrConfigs()
    {
        availableFrConfigs.clear();
    }

    @VisibleForTesting
    public static synchronized void addFrConfigs(String profileName, Properties properties)
    {
        availableFrConfigs.putIfAbsent(profileName, properties);
    }

    public String getFailureRetryPolicyUserProfile()
    {
        return this.failureRetryPolicyConfig;
    }

    public void loadFactoryConfigs()
            throws IOException
    {
        LOG.info("-- Available failure retry policy factories: %s --", failureRetryFactories.keySet().toString());

        File configDir = new File(FD_RETRY_CONFIG_DIR);
        if (!configDir.exists() || !configDir.isDirectory()) {
            LOG.info("-- failure retry policy configs not found. Skipped loading --");
            return;
        }

        String[] failureRetries = configDir.list();

        if (failureRetries == null || failureRetries.length == 0) {
            LOG.info("-- no retry policy set. Default failure retry policy will be used. --");
            return;
        }

        for (String fileName : failureRetries) {
            if (fileName.endsWith(".properties")) {
                String configName = fileName.replaceAll("\\.properties", "");
                File configFile = new File(FD_RETRY_CONFIG_DIR + fileName);
                Properties properties = loadProperties(configFile);

                String configType = properties.getProperty(FailureRetryPolicy.FD_RETRY_TYPE);
                checkState(configType != null, "%s must be specified in %s",
                        FailureRetryPolicy.FD_RETRY_TYPE, configFile.getCanonicalPath());
                checkState(failureRetryFactories.containsKey(configType),
                        "Factory for failure retry policy type %s not found", configType);

                availableFrConfigs.put(configName, properties);
                LOG.info(String.format("Loaded '%s' failure retry policy config '%s'", configType, configName));

                LOG.info(String.format("-- Loaded failure retry profiles: %s --",
                        availableFrConfigs.keySet().toString()));
            }
        }
    }

    private Properties loadProperties(File configFile)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(configFile)) {
            properties.load(in);
        }
        return properties;
    }

    public FailureRetryPolicy getFailureRetryPolicy(String name)
    {
        Properties frConfig = availableFrConfigs.get(name);
        LOG.debug("Profile name: " + name + ", retry type: " + frConfig.getProperty(FailureRetryPolicy.FD_RETRY_TYPE));
        return getFailureRetryPolicy(frConfig);
    }

    private String checkProperty(Properties properties, String key)
    {
        String val = properties.getProperty(key);
        LOG.debug("Key: " + key + ", value: " + val);
        if (val == null) {
            throw new IllegalArgumentException(String.format("Configuration entry '%s' must be specified", key));
        }
        return val;
    }

    private FailureRetryPolicy getFailureRetryPolicy(Properties properties)
    {
        String type = checkProperty(properties, FailureRetryPolicy.FD_RETRY_TYPE);
        checkState(failureRetryFactories.containsKey(type),
                "Factory for failure retry policy type %s not found", type);
        FailureRetryFactory factory = failureRetryFactories.get(type);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.getFailureRetryPolicy(properties);
        }
    }

    @VisibleForTesting
    public FailureRetryPolicy getFailureRetryPolicy(String name, Ticker ticker)
    {
        Properties frConfig = availableFrConfigs.get(name);
        String type = checkProperty(frConfig, FailureRetryPolicy.FD_RETRY_TYPE);
        checkState(failureRetryFactories.containsKey(type),
                "Factory for failure retry policy type %s not found", type);
        FailureRetryFactory factory = failureRetryFactories.get(type);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.getFailureRetryPolicy(frConfig, ticker);
        }
    }
}
