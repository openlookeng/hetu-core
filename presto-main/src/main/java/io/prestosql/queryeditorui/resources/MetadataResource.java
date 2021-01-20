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
package io.prestosql.queryeditorui.resources;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.prestosql.client.Column;
import io.prestosql.queryeditorui.metadata.ColumnService;
import io.prestosql.queryeditorui.metadata.PreviewTableService;
import io.prestosql.queryeditorui.metadata.SchemaService;
import io.prestosql.queryeditorui.protocol.CatalogSchema;
import io.prestosql.queryeditorui.protocol.Table;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Path("/api/metadata")
public class MetadataResource
{
    private final SchemaService schemaService;
    private final ColumnService columnService;
    private final PreviewTableService previewTableService;

    @Inject
    public MetadataResource(
            final SchemaService schemaService,
            final ColumnService columnService,
            final PreviewTableService previewTableService)
    {
        this.schemaService = schemaService;
        this.columnService = columnService;
        this.previewTableService = previewTableService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("tables/{catalog}/{schema}")
    public Response getTables(
            @PathParam("catalog") String catalogName,
            @PathParam("schema") String schemaName,
            @Context HttpServletRequest servletRequest)
    {
        ImmutableList<Table> result = schemaService.queryTables(catalogName, schemaName, servletRequest);
        return Response.ok(result).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("columns/{catalog}/{schema}/{table}")
    public Response getTableColumns(
            @PathParam("catalog") String catalogName,
            @PathParam("schema") String schemaName,
            @PathParam("table") String tableName,
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        List<Column> columnList = columnService.getColumns(catalogName, schemaName, tableName, servletRequest);
        return Response.ok(columnList).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("preview/{catalog}/{schema}/{table}")
    public Response getTablePreview(
            @PathParam("catalog") String catalogName,
            @PathParam("schema") String schemaName,
            @PathParam("table") String tableName,
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        List<List<Object>> preview = previewTableService.getPreview(catalogName, schemaName, tableName, servletRequest);
        return Response.ok(preview).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("schemas")
    public Response getSchemas(
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        ImmutableList<CatalogSchema> result = schemaService.querySchemas(servletRequest);
        return Response.ok(result).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("schemas/{catalog}")
    public Response getSchemasFromCatalog(
            @PathParam("catalog") String catalogName,
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        CatalogSchema catalogSchema = schemaService.querySchemas(catalogName, servletRequest);
        return Response.ok(catalogSchema).build();
    }
}
