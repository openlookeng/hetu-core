/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.catalog.showcatalog;

import com.google.inject.Inject;
import io.prestosql.spi.security.GroupProvider;
import org.assertj.core.util.VisibleForTesting;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.Map;

import static io.prestosql.catalog.DynamicCatalogService.badRequest;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/showCatalog")
public class ShowCatalogResource
{
    private static final int MAX_NAME_LENGTH = 255;
    private static final String VALID_CATALOG_NAME_REGEX = "[\\p{Alnum}_]+";
    private final ShowCatalogService service;
    private final GroupProvider groupProvider;

    @Inject
    public ShowCatalogResource(ShowCatalogService service, GroupProvider groupProvider)
    {
        this.service = requireNonNull(service, "service is null");
        this.groupProvider = groupProvider;
    }

    @VisibleForTesting
    void checkCatalogName(String catalogName)
    {
        if (catalogName.length() > MAX_NAME_LENGTH) {
            throw badRequest(BAD_REQUEST, "The length of catalog name is too long");
        }

        // check dc
        int dotIndex = catalogName.indexOf(".");
        if (dotIndex >= 0) {
            if (dotIndex == 0 || dotIndex == catalogName.length() - 1) {
                throw badRequest(BAD_REQUEST, "Invalid catalog name");
            }
            String dc = catalogName.substring(0, dotIndex);
            String catalog = catalogName.substring(dotIndex + 1);
            if (!dc.matches(VALID_CATALOG_NAME_REGEX) || !catalog.matches(VALID_CATALOG_NAME_REGEX)) {
                throw badRequest(BAD_REQUEST, "Invalid catalog name");
            }
            return;
        }

        if (!catalogName.matches(VALID_CATALOG_NAME_REGEX)) {
            throw badRequest(BAD_REQUEST, "Invalid catalog name");
        }
    }

    @GET
    @Path("/{catalogName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getCatalogpropertites(@NotNull @PathParam("catalogName") String catalogName,
                                                     @Context HttpServletRequest servletRequest)
    {
        checkCatalogName(catalogName);
        Map<String, String> response;
        try {
            response = service.getCatalogpropertites(catalogName);
        }
        catch (WebApplicationException ex) {
            throw ex;
        }
        catch (Throwable ex) {
            throw badRequest(BAD_REQUEST, "show catalog failed. please check your request info.");
        }

        return response;
    }
}
