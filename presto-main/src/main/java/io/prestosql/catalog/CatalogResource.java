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

package io.prestosql.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.prestosql.server.HttpRequestSessionContext;
import io.prestosql.spi.security.GroupProvider;
import org.assertj.core.util.VisibleForTesting;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.annotation.MultipartConfig;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static io.prestosql.catalog.CatalogFileInputStream.CatalogFileType.CATALOG_FILE;
import static io.prestosql.catalog.CatalogFileInputStream.CatalogFileType.GLOBAL_FILE;
import static io.prestosql.catalog.DynamicCatalogService.badRequest;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/catalog")
@MultipartConfig(maxFileSize = 20971520, maxRequestSize = 20971520)
public class CatalogResource
{
    private static final int MAX_NAME_LENGTH = 255;
    private static final JsonCodec<CatalogInfo> CATALOG_INFO_CODEC = JsonCodec.jsonCodec(CatalogInfo.class);
    private static final String VALID_CATALOG_NAME_REGEX = "[\\p{Alnum}_]+";
    private static final String VALID_FILE_NAME_REGEX = "[^\\s\\\\/:\\*\\?\\\"<>\\|](\\x20|[^\\s\\\\/:\\*\\?\\\"<>\\|])*[^\\s\\\\/:\\*\\?\\\"<>\\|\\.]$";
    private final DynamicCatalogService service;
    private final int catalogMaxFileSizeInBytes;
    private final int catalogMaxFileNumber;
    private final List<String> validFileSuffixes;
    private final GroupProvider groupProvider;

    @Inject
    public CatalogResource(DynamicCatalogService service, DynamicCatalogConfig config, GroupProvider groupProvider)
    {
        requireNonNull(config, "config is null");
        this.service = requireNonNull(service, "service is null");
        catalogMaxFileSizeInBytes = (int) config.getCatalogMaxFileSize().toBytes();
        catalogMaxFileNumber = config.getCatalogMaxFileNumber();
        String suffixes = config.getCatalogValidFileSuffixes();
        validFileSuffixes = (suffixes == null ? ImmutableList.of() : Arrays.asList(suffixes.split(",")));
        this.groupProvider = groupProvider;
    }

    @VisibleForTesting
    boolean checkFileName(String fileName)
    {
        if (fileName.length() > MAX_NAME_LENGTH) {
            return false;
        }

        if (!fileName.matches(VALID_FILE_NAME_REGEX)) {
            return false;
        }

        String suffix = Files.getFileExtension(fileName);
        if (validFileSuffixes.isEmpty() || validFileSuffixes.contains(suffix)) {
            return true;
        }
        return false;
    }

    private void putInputStreams(CatalogFileInputStream.Builder builder, List<FormDataBodyPart> configFileBodyParts, CatalogFileInputStream.CatalogFileType fileType)
            throws IOException
    {
        if (configFileBodyParts != null) {
            if (configFileBodyParts.size() > catalogMaxFileNumber) {
                throw new IOException("The number of config file is greater than " + catalogMaxFileNumber);
            }
            for (FormDataBodyPart bodyPart : configFileBodyParts) {
                BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyPart.getEntity();
                String fileName = bodyPart.getContentDisposition().getFileName();
                if (!checkFileName(fileName)) {
                    builder.close();
                    throw new IOException("The file name is not correct.");
                }
                InputStream bodyPartInputStream = bodyPartEntity.getInputStream();
                builder.put(fileName, fileType, bodyPartInputStream);
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

    private CatalogInfo toCatalogInfo(String catalogInfoJson)
    {
        if (catalogInfoJson == null) {
            throw badRequest(BAD_REQUEST, "Catalog information is missing");
        }

        try {
            CatalogInfo catalogInfo = CATALOG_INFO_CODEC.fromJson(catalogInfoJson);
            checkCatalogName(catalogInfo.getCatalogName());
            return catalogInfo;
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
                    new HttpRequestSessionContext(servletRequest, groupProvider));
        }
        catch (WebApplicationException ex) {
            throw ex;
        }
        catch (Throwable ex) {
            throw badRequest(BAD_REQUEST, "create catalog failed. please check your configuration.");
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
                    new HttpRequestSessionContext(servletRequest, groupProvider));
        }
        catch (WebApplicationException ex) {
            throw ex;
        }
        catch (Throwable ex) {
            throw badRequest(BAD_REQUEST, "update catalog failed. please check your configuration.");
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
        checkCatalogName(catalogName);
        Response response;
        try {
            response = service.dropCatalog(catalogName, new HttpRequestSessionContext(servletRequest, groupProvider));
        }
        catch (WebApplicationException ex) {
            throw ex;
        }
        catch (Throwable ex) {
            throw badRequest(BAD_REQUEST, "drop catalog failed. please check your request info.");
        }

        return response;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response showCatalogs(@Context HttpServletRequest servletRequest)
    {
        Response response;
        try {
            response = service.showCatalogs(new HttpRequestSessionContext(servletRequest, groupProvider));
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
