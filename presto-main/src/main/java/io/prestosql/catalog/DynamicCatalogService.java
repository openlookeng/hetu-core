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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.connector.DataCenterConnectorManager;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlUtil;
import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.SecurityKeyException;
import io.prestosql.spi.security.SecurityKeyManager;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import static io.prestosql.catalog.DynamicCatalogStore.CatalogStoreType.SHARE;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FOUND;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

public class DynamicCatalogService
{
    private static final Logger log = Logger.get(DynamicCatalogService.class);
    private final CatalogManager catalogManager;
    private final DynamicCatalogStore dynamicCatalogStore;
    private final AccessControl accessControl;
    private final SecurityKeyManager securityKeyManager;
    private final DataCenterConnectorManager dataCenterConnectorManager;

    @Inject
    public DynamicCatalogService(CatalogManager catalogManager, DynamicCatalogStore dynamicCatalogStore, AccessControl accessControl,
                                 SecurityKeyManager securityKeyManager, DataCenterConnectorManager dataCenterConnectorManager)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.dynamicCatalogStore = requireNonNull(dynamicCatalogStore, "dynamicCatalogStore is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.securityKeyManager = securityKeyManager;
        this.dataCenterConnectorManager = dataCenterConnectorManager;
    }

    public static WebApplicationException badRequest(Response.Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private Lock tryLock(String catalogName)
            throws IOException
    {
        Lock lock = dynamicCatalogStore.getCatalogLock(catalogName);
        if (!lock.tryLock()) {
            throw badRequest(CONFLICT, "There are other requests operating this catalog");
        }
        return lock;
    }

    private void deleteSecurityKey(String catalogName)
    {
        try {
            securityKeyManager.deleteKey(catalogName);
        }
        catch (SecurityKeyException e) {
            // if error happen, just log it. the alias can be rewrite when create a same key, so we can ignore this kind error
            log.warn("Delete security key of %s failed, cause: %s.", catalogName, e.getMessage());
        }
    }

    private boolean isCatalogExist(String catalogName)
            throws IOException
    {
        return dynamicCatalogStore.listCatalogNames(SHARE).contains(catalogName);
    }

    public synchronized Response createCatalog(CatalogInfo catalogInfo,
                                               CatalogFileInputStream configFiles,
                                               HttpRequestSessionContext sessionContext)
            throws IOException
    {
        String catalogName = catalogInfo.getCatalogName();

        // check the permission.
        try {
            AccessControlUtil.checkCanImpersonateUser(accessControl, sessionContext);
            accessControl.checkCanCreateCatalog(sessionContext.getIdentity(), catalogName);
        }
        catch (Exception ex) {
            throw badRequest(UNAUTHORIZED, "No permission");
        }

        Lock lock = tryLock(catalogName);
        try {
            // check this catalog exists or not, if this catalog has existed in the share file system, return catalog is exist.
            if (catalogManager.getCatalog(catalogName).isPresent() || isCatalogExist(catalogName)) {
                throw badRequest(FOUND, "The catalog [" + catalogName + "] already exists");
            }

            boolean needSaveKey = catalogInfo.getSecurityKey() != null && !catalogInfo.getSecurityKey().isEmpty();
            // save security key
            if (needSaveKey) {
                try {
                    securityKeyManager.saveKey(catalogInfo.getSecurityKey().toCharArray(), catalogName);
                }
                catch (SecurityKeyException e) {
                    throw badRequest(BAD_REQUEST, "Failed to save key.");
                }
            }

            // create catalog
            try {
                // load catalog and store related configuration files to share file system.
                dynamicCatalogStore.loadCatalogAndCreateShareFiles(catalogInfo, configFiles);
            }
            catch (PrestoException | IllegalArgumentException ex) {
                if (needSaveKey) {
                    deleteSecurityKey(catalogName);
                }
                throw badRequest(BAD_REQUEST, "Failed to load catalog. Please check your configuration.");
            }
        }
        finally {
            lock.unlock();
        }
        return Response.status(CREATED).build();
    }

    private void rollbackKey(String catalogName, char[] key)
            throws IOException
    {
        try {
            securityKeyManager.deleteKey(catalogName);
            securityKeyManager.saveKey(key, catalogName);
        }
        catch (SecurityKeyException e) {
            String message = String.format("Update %s failed and rollback key failed.", catalogName);
            log.error(message);
            throw new IOException(message);
        }
    }

    public synchronized Response updateCatalog(CatalogInfo catalogInfo,
                                               CatalogFileInputStream configFiles,
                                               HttpRequestSessionContext sessionContext)
            throws IOException
    {
        String catalogName = catalogInfo.getCatalogName();

        // check the permission.
        try {
            AccessControlUtil.checkCanImpersonateUser(accessControl, sessionContext);
            accessControl.checkCanUpdateCatalog(sessionContext.getIdentity(), catalogName);
        }
        catch (Exception ex) {
            throw badRequest(UNAUTHORIZED, "No permission");
        }

        Lock lock = tryLock(catalogName);
        try {
            // check this catalog exists.
            if (!isCatalogExist(catalogName)) {
                throw badRequest(NOT_FOUND, "The catalog [" + catalogName + "] does not exist");
            }

            // update security key
            boolean updateKey = (catalogInfo.getSecurityKey() != null);
            char[] preSecurityKey = null;
            if (updateKey) {
                try {
                    preSecurityKey = securityKeyManager.getKey(catalogName);
                    securityKeyManager.saveKey(catalogInfo.getSecurityKey().toCharArray(), catalogName);
                }
                catch (SecurityKeyException e) {
                    throw badRequest(BAD_REQUEST, "Failed to update catalog. Please check your configuration.");
                }
            }

            // update catalog
            try {
                // update the catalog and update related configuration files in the share file system.
                dynamicCatalogStore.updateCatalogAndShareFiles(catalogInfo, configFiles);
            }
            catch (PrestoException | IllegalArgumentException ex) {
                if (updateKey) {
                    if (preSecurityKey != null) {
                        rollbackKey(catalogName, preSecurityKey);
                    }
                    else {
                        deleteSecurityKey(catalogName);
                    }
                }
                throw badRequest(BAD_REQUEST, "Failed to update catalog. Please check your configuration.");
            }
        }
        finally {
            lock.unlock();
        }

        return Response.status(CREATED).build();
    }

    public synchronized Response dropCatalog(String catalogName, HttpRequestSessionContext sessionContext)
            throws IOException
    {
        // check the permission.
        try {
            AccessControlUtil.checkCanImpersonateUser(accessControl, sessionContext);
            accessControl.checkCanDropCatalog(sessionContext.getIdentity(), catalogName);
        }
        catch (Exception ex) {
            throw badRequest(UNAUTHORIZED, "No permission");
        }

        // datacenter catalog
        if (catalogName.indexOf(".") > 0) {
            String dc = catalogName.substring(0, catalogName.indexOf("."));
            if (!dataCenterConnectorManager.isDCCatalog(dc)) {
                throw badRequest(NOT_FOUND, "The datacenter [" + dc + "] does not exist");
            }

            Lock lock = tryLock(dc);
            try {
                dataCenterConnectorManager.dropDCConnection(dc);
                deleteSecurityKey((dc));
                dynamicCatalogStore.deleteCatalogShareFiles(dc);
            }
            finally {
                lock.unlock();
            }
            return Response.status(NO_CONTENT).build();
        }

        Lock lock = tryLock(catalogName);
        try {
            // check this catalog exists.
            if (!isCatalogExist(catalogName)) {
                throw badRequest(NOT_FOUND, "The catalog [" + catalogName + "] does not exist");
            }

            // delete security key
            deleteSecurityKey(catalogName);

            // delete from share file system.
            dynamicCatalogStore.deleteCatalogShareFiles(catalogName);
        }
        finally {
            lock.unlock();
        }

        return Response.status(NO_CONTENT).build();
    }

    public Response showCatalogs(HttpRequestSessionContext sessionContext)
            throws IOException
    {
        Set<String> catalogNames = dynamicCatalogStore.listCatalogNames(SHARE);
        Set<String> allowedCatalogs;
        try {
            AccessControlUtil.checkCanImpersonateUser(accessControl, sessionContext);
            allowedCatalogs = accessControl.filterCatalogs(sessionContext.getIdentity(), catalogNames);
            return Response.ok(allowedCatalogs).build();
        }
        catch (Exception e) {
            log.error("Filter catalogs error : %s.", e.getMessage());
            throw badRequest(UNAUTHORIZED, "No permission");
        }
    }
}
