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

package io.prestosql.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.metadata.DiscoveryNodeManager;
import io.prestosql.spi.PrestoException;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.catalog.DynamicCatalogStore.CatalogStoreType.LOCAL;
import static io.prestosql.catalog.DynamicCatalogStore.CatalogStoreType.SHARE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class DynamicCatalogScanner
{
    private static final Logger log = Logger.get(DiscoveryNodeManager.class);
    private final DynamicCatalogStore dynamicCatalogStore;
    private final ScheduledExecutorService catalogScannerExecutor;
    private final Duration catalogScannerInterval;
    private final boolean dynamicCatalogEnabled;
    private final ConcurrentMap<String, Boolean> exceptionPrintFlags = new ConcurrentHashMap<>();

    @Inject
    public DynamicCatalogScanner(DynamicCatalogStore dynamicCatalogStore, DynamicCatalogConfig dynamicCatalogConfig)
    {
        this.dynamicCatalogStore = requireNonNull(dynamicCatalogStore, "dynamicCatalogStore is null");
        this.catalogScannerExecutor = newSingleThreadScheduledExecutor(threadsNamed("dynamic-catalog-scanner-%s"));
        this.catalogScannerInterval = dynamicCatalogConfig.getCatalogScannerInterval();
        this.dynamicCatalogEnabled = dynamicCatalogConfig.isDynamicCatalogEnabled();
    }

    public void start()
    {
        if (!dynamicCatalogEnabled) {
            log.info("Dynamic catalog feature is disabled");
            return;
        }

        checkState(!catalogScannerExecutor.isShutdown(), "Dynamic catalog scanner has been destroyed");
        log.info("Start to scan the remote catalogs");

        // start scanner.
        catalogScannerExecutor.scheduleWithFixedDelay(() -> {
            try {
                scan();
            }
            catch (IOException | PrestoException ex) {
                log.error(ex, "Error scanning dynamic catalog");
            }
        }, 0, catalogScannerInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy()
    {
        catalogScannerExecutor.shutdownNow();
        try {
            catalogScannerExecutor.awaitTermination(30, SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @VisibleForTesting
    void scan()
            throws IOException
    {
        // list all catalogs from Share File System.
        Set<String> remoteCatalogNames = dynamicCatalogStore.listCatalogNames(SHARE);

        // list all catalogs in the local disk.
        Set<String> localCatalogNames = dynamicCatalogStore.listCatalogNames(LOCAL);

        // add new catalogs.
        Set<String> newCatalogNames = difference(remoteCatalogNames, localCatalogNames);
        for (String newCatalogName : newCatalogNames) {
            log.info("-- Need add catalog [%s] --", newCatalogName);
            try {
                dynamicCatalogStore.loadCatalog(newCatalogName);
                exceptionPrintFlags.put(newCatalogName, false);
            }
            catch (Throwable ignore) {
                exceptionPrint(newCatalogName, String.format("Scanner try to load catalog [%s] failed", newCatalogName), ignore);
            }
        }

        // unload catalogs.
        Set<String> deleteCatalogNames = difference(localCatalogNames, remoteCatalogNames);
        for (String deleteCatalogName : deleteCatalogNames) {
            log.info("-- Need remove catalog [%s] --", deleteCatalogName);
            try {
                dynamicCatalogStore.unloadCatalog(deleteCatalogName);
                exceptionPrintFlags.put(deleteCatalogName, false);
            }
            catch (Throwable ignore) {
                exceptionPrint(deleteCatalogName, String.format("Scanner try to unload catalog [%s] failed", deleteCatalogName), ignore);
            }
        }

        // get local catalog names again, and compare the catalog version.
        localCatalogNames = dynamicCatalogStore.listCatalogNames(LOCAL);
        localCatalogNames
                .forEach(catalogName -> {
                    try {
                        // if can not get the version from local disk, we will reload this catalog.
                        String localVersion;
                        try {
                            localVersion = dynamicCatalogStore.getCatalogVersion(catalogName, LOCAL);
                        }
                        catch (IOException e) {
                            exceptionPrint(catalogName, String.format("Get local catalog version of [%s] failed", catalogName), e);
                            localVersion = "";
                        }
                        String remoteVersion = dynamicCatalogStore.getCatalogVersion(catalogName, SHARE);
                        if (!localVersion.equals(remoteVersion)) {
                            log.info("-- Need update catalog [%s], caused by the local version [%s] is not equal the remote version [%s] --",
                                    catalogName,
                                    localVersion,
                                    remoteVersion);
                            dynamicCatalogStore.reloadCatalog(catalogName);
                            exceptionPrintFlags.put(catalogName, false);
                        }
                    }
                    catch (IOException e) {
                        exceptionPrint(catalogName, String.format("Get remote catalog version of [%s] failed", catalogName), e);
                    }
                    catch (Throwable e) {
                        exceptionPrint(catalogName, "Reload failed", e);
                    }
                });

        Set<String> allCatalogNames = new HashSet<>();
        allCatalogNames.addAll(remoteCatalogNames);
        allCatalogNames.addAll(localCatalogNames);
        Set<String> expiredCatalogNames = difference(exceptionPrintFlags.keySet(), allCatalogNames);
        expiredCatalogNames.forEach(catalogName -> exceptionPrintFlags.remove(catalogName));
    }

    void exceptionPrint(String catalogName, String message, Throwable cause)
    {
        if (exceptionPrintFlags.get(catalogName) == null || !exceptionPrintFlags.get(catalogName)) {
            log.warn(cause, message);
        }
        exceptionPrintFlags.put(catalogName, true);
    }
}
