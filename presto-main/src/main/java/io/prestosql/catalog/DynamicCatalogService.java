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
import io.prestosql.metadata.CatalogManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.spi.PrestoException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FOUND;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

public class DynamicCatalogService
{
    private static final Logger log = Logger.get(DynamicCatalogService.class);
    private final CatalogManager catalogManager;
    private final DynamicCatalogStore dynamicCatalogStore;
    private final AccessControl accessControl;
    private final CatalogStoreUtil catalogStoreUtil;

    @Inject
    public DynamicCatalogService(CatalogManager catalogManager, DynamicCatalogStore dynamicCatalogStore, AccessControl accessControl, CatalogStoreUtil catalogStoreUtil)
    {
        this.catalogManager = catalogManager;
        this.dynamicCatalogStore = dynamicCatalogStore;
        this.accessControl = accessControl;
        this.catalogStoreUtil = catalogStoreUtil;
    }

    public static WebApplicationException badRequest(Response.Status status, String message)
    {
        log.error(message);
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private Lock tryLock(CatalogStore shareCatalogStore, String catalogName)
            throws IOException
    {
        Lock lock = dynamicCatalogStore.getCatalogLock(shareCatalogStore, catalogName);
        if (!lock.tryLock()) {
            throw badRequest(CONFLICT, "There are other requests operating this catalog");
        }
        return lock;
    }

    public synchronized Response createCatalog(CatalogInfo catalogInfo,
            CatalogFileInputStream configFiles,
            HttpRequestSessionContext sessionContext)
    {
        String catalogName = catalogInfo.getCatalogName();
        // check the permission.
        try {
            accessControl.checkCanCreateCatalog(sessionContext.getIdentity(), catalogName);
        }
        catch (Exception ex) {
            throw badRequest(UNAUTHORIZED, "No permission");
        }

        try (CatalogStore localCatalogStore = catalogStoreUtil.getLocalCatalogStore();
                CatalogStore shareCatalogStore = catalogStoreUtil.getShareCatalogStore()) {
            Lock lock = tryLock(shareCatalogStore, catalogName);
            try {
                // check this catalog exists or not, if this catalog has existed in the share file system, return catalog is exist.
                boolean isExistInRemoteStore;
                try {
                    isExistInRemoteStore = dynamicCatalogStore.listCatalogNames(shareCatalogStore).contains(catalogName);
                }
                catch (IOException ex) {
                    throw badRequest(INTERNAL_SERVER_ERROR, "Failed to list existing catalogs");
                }

                if (catalogManager.getCatalog(catalogName).isPresent() || isExistInRemoteStore) {
                    throw badRequest(FOUND, "The catalog [" + catalogName + "] already exists");
                }

                try {
                    // load catalog and store related configuration files to share file system.
                    dynamicCatalogStore.loadCatalogAndCreateShareFiles(localCatalogStore, shareCatalogStore, catalogInfo, configFiles);
                }
                catch (PrestoException ex) {
                    throw badRequest(INTERNAL_SERVER_ERROR, ex.getMessage());
                }
                catch (IllegalArgumentException ex) {
                    throw badRequest(BAD_REQUEST, ex.getMessage());
                }
            }
            finally {
                if (lock != null) {
                    lock.unlock();
                }
            }
        }
        catch (IOException ex) {
            throw badRequest(INTERNAL_SERVER_ERROR, ex.getMessage());
        }

        return Response.status(CREATED).build();
    }

    public synchronized Response updateCatalog(CatalogInfo catalogInfo,
            CatalogFileInputStream configFiles,
            HttpRequestSessionContext sessionContext)
    {
        String catalogName = catalogInfo.getCatalogName();

        // check the permission.
        try {
            accessControl.checkCanDropCatalog(sessionContext.getIdentity(), catalogName);
        }
        catch (Exception ex) {
            throw badRequest(UNAUTHORIZED, "No permission");
        }

        try (CatalogStore localCatalogStore = catalogStoreUtil.getLocalCatalogStore();
                CatalogStore shareCatalogStore = catalogStoreUtil.getShareCatalogStore()) {
            Lock lock = tryLock(shareCatalogStore, catalogName);
            try {
                // check this catalog exists.
                boolean isExistInRemoteStore;
                try {
                    isExistInRemoteStore = dynamicCatalogStore.listCatalogNames(shareCatalogStore).contains(catalogName);
                }
                catch (IOException ex) {
                    throw badRequest(INTERNAL_SERVER_ERROR, "Failed to list existing catalogs");
                }

                if (!isExistInRemoteStore) {
                    throw badRequest(NOT_FOUND, "The catalog [" + catalogName + "] does not exist");
                }

                // update catalog
                try {
                    // update the catalog and update related configuration files in the share file system.
                    dynamicCatalogStore.updateCatalogAndShareFiles(localCatalogStore, shareCatalogStore, catalogInfo, configFiles);
                }
                catch (PrestoException ex) {
                    throw badRequest(INTERNAL_SERVER_ERROR, ex.getMessage());
                }
                catch (IllegalArgumentException ex) {
                    throw badRequest(BAD_REQUEST, ex.getMessage());
                }
            }
            finally {
                if (lock != null) {
                    lock.unlock();
                }
            }
        }
        catch (IOException ex) {
            throw badRequest(INTERNAL_SERVER_ERROR, ex.getMessage());
        }

        return Response.status(CREATED).build();
    }

    public synchronized Response dropCatalog(String catalogName, HttpRequestSessionContext sessionContext)
    {
        // check the permission.
        try {
            accessControl.checkCanDropCatalog(sessionContext.getIdentity(), catalogName);
        }
        catch (Exception ex) {
            throw badRequest(UNAUTHORIZED, "No permission");
        }

        try (CatalogStore shareCatalogStore = catalogStoreUtil.getShareCatalogStore()) {
            Lock lock = tryLock(shareCatalogStore, catalogName);
            try {
                // check this catalog exists.
                boolean isExistInRemoteStore;
                try {
                    isExistInRemoteStore = dynamicCatalogStore.listCatalogNames(shareCatalogStore).contains(catalogName);
                }
                catch (IOException ex) {
                    throw badRequest(INTERNAL_SERVER_ERROR, "Failed to list existing catalogs");
                }

                if (!isExistInRemoteStore) {
                    throw badRequest(NOT_FOUND, "The catalog [" + catalogName + "] does not exist");
                }

                // delete from share file system.
                dynamicCatalogStore.deleteCatalogShareFiles(shareCatalogStore, catalogName);
                // release lock.
                dynamicCatalogStore.releaseCatalogLock(shareCatalogStore, catalogName);
            }
            finally {
                if (lock != null) {
                    lock.unlock();
                }
            }
        }
        catch (IOException ex) {
            throw badRequest(INTERNAL_SERVER_ERROR, ex.getMessage());
        }

        return Response.status(NO_CONTENT).build();
    }

    public Response showCatalogs()
    {
        try (CatalogStore shareCatalogStore = catalogStoreUtil.getShareCatalogStore()) {
            Set<String> catalogNames = dynamicCatalogStore.listCatalogNames(shareCatalogStore);
            return Response.ok(catalogNames).build();
        }
        catch (IOException ex) {
            throw badRequest(INTERNAL_SERVER_ERROR, ex.getMessage());
        }
    }
}
