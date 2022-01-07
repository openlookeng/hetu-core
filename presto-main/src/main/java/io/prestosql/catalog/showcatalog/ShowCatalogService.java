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
import io.prestosql.server.HttpRequestSessionContext;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

public class ShowCatalogService
{
    private final ShowCatalogStore showCatalogStore;

    @Inject
    public ShowCatalogService(ShowCatalogStore showCatalogStore)
    {
        this.showCatalogStore = requireNonNull(showCatalogStore, "dynamicCatalogStore is null");
    }

    public static WebApplicationException badRequest(Response.Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    public Map<String, String> getCatalogpropertites(HttpRequestSessionContext sessionContext, String catalogname)
            throws IOException
    {
        return showCatalogStore.getCatalogProperties(catalogname);
    }
}
