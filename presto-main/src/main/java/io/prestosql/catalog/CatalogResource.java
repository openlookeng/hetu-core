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
import io.airlift.json.JsonCodec;
import io.prestosql.server.HttpRequestSessionContext;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static io.prestosql.catalog.CatalogFileInputStream.CatalogFileType.CATALOG_FILE;
import static io.prestosql.catalog.CatalogFileInputStream.CatalogFileType.GLOBAL_FILE;
import static io.prestosql.catalog.DynamicCatalogService.badRequest;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/v1/catalog")
public class CatalogResource
{
    private static final JsonCodec<CatalogInfo> CATALOG_INFO_CODEC = JsonCodec.jsonCodec(CatalogInfo.class);

    private final DynamicCatalogService service;
    private final int catalogMaxFileSizeInBytes;

    @Inject
    public CatalogResource(DynamicCatalogService service, DynamicCatalogConfig config)
    {
        requireNonNull(config, "config is null");
        this.service = requireNonNull(service, "service is null");
        catalogMaxFileSizeInBytes = (int) config.getCatalogMaxFileSize().toBytes();
    }

    private void putInputStreams(CatalogFileInputStream.Builder builder, List<FormDataBodyPart> configFileBodyParts, CatalogFileInputStream.CatalogFileType fileType)
            throws IOException
    {
        if (configFileBodyParts != null) {
            for (FormDataBodyPart bodyPart : configFileBodyParts) {
                BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyPart.getEntity();
                String fileName = bodyPart.getContentDisposition().getFileName();
                InputStream bodyPartInputStream = bodyPartEntity.getInputStream();
                try {
                    builder.put(fileName, fileType, bodyPartInputStream);
                }
                catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    private CatalogFileInputStream toCatalogFiles(List<FormDataBodyPart> catalogConfigFileBodyParts, List<FormDataBodyPart> globalConfigFilesBodyParts)
            throws IOException
    {
        CatalogFileInputStream.Builder builder = new CatalogFileInputStream.Builder(catalogMaxFileSizeInBytes);
        putInputStreams(builder, catalogConfigFileBodyParts, CATALOG_FILE);
        putInputStreams(builder, globalConfigFilesBodyParts, GLOBAL_FILE);
        return builder.build();
    }

    private void closeInputStreams(List<FormDataBodyPart> configFileBodyParts)
    {
        if (configFileBodyParts != null) {
            configFileBodyParts.stream()
                    .map(bodyPart -> ((BodyPartEntity) bodyPart.getEntity()))
                    .map(BodyPartEntity::getInputStream)
                    .forEach(inputStream -> {
                        try {
                            inputStream.close();
                        }
                        catch (IOException e) {
                        }
                    });
        }
    }

    private CatalogInfo toCatalogInfo(String catalogInfoJson)
    {
        if (catalogInfoJson == null) {
            throw badRequest(BAD_REQUEST, "Catalog information is missing");
        }

        try {
            return CATALOG_INFO_CODEC.fromJson(catalogInfoJson);
        }
        catch (IllegalArgumentException ex) {
            throw badRequest(BAD_REQUEST, "Invalid JSON string of catalog information");
        }
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createCatalog(@FormDataParam("catalogInformation") String catalogInfoJson,
            @FormDataParam("catalogConfigurationFiles") List<FormDataBodyPart> catalogConfigFileBodyParts,
            @FormDataParam("globalConfigurationFiles") List<FormDataBodyPart> globalConfigFilesBodyParts,
            @Context HttpServletRequest servletRequest)
    {
        CatalogInfo catalogInfo = toCatalogInfo(catalogInfoJson);

        try (CatalogFileInputStream configFiles = toCatalogFiles(catalogConfigFileBodyParts, globalConfigFilesBodyParts)) {
            return service.createCatalog(catalogInfo,
                    configFiles,
                    new HttpRequestSessionContext(servletRequest));
        }
        catch (IOException ex) {
            throw badRequest(INTERNAL_SERVER_ERROR, "Files are not invalid");
        }
        finally {
            closeInputStreams(catalogConfigFileBodyParts);
            closeInputStreams(globalConfigFilesBodyParts);
        }
    }

    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateCatalog(@FormDataParam("catalogInformation") String catalogInfoJson,
            @FormDataParam("catalogConfigurationFiles") List<FormDataBodyPart> catalogConfigFileBodyParts,
            @FormDataParam("globalConfigurationFiles") List<FormDataBodyPart> globalConfigFilesBodyParts,
            @Context HttpServletRequest servletRequest)
    {
        CatalogInfo catalogInfo = toCatalogInfo(catalogInfoJson);

        try (CatalogFileInputStream configFiles = toCatalogFiles(catalogConfigFileBodyParts, globalConfigFilesBodyParts)) {
            return service.updateCatalog(catalogInfo,
                    configFiles,
                    new HttpRequestSessionContext(servletRequest));
        }
        catch (IOException ex) {
            throw badRequest(INTERNAL_SERVER_ERROR, "Files are not invalid");
        }
        finally {
            closeInputStreams(catalogConfigFileBodyParts);
            closeInputStreams(globalConfigFilesBodyParts);
        }
    }

    @DELETE
    @Path("/{catalogName}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response dropCatalog(@NotNull @PathParam("catalogName") String catalogName,
            @Context HttpServletRequest servletRequest)
    {
        return service.dropCatalog(catalogName, new HttpRequestSessionContext(servletRequest));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response showCatalogs(@Context HttpServletRequest servletRequest)
    {
        return service.showCatalogs(new HttpRequestSessionContext(servletRequest));
    }
}
