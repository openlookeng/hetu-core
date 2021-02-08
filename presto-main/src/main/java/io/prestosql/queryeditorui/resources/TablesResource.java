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
import com.google.inject.name.Named;
import io.prestosql.client.Column;
import io.prestosql.connector.DataCenterConnectorManager;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.queryeditorui.metadata.ColumnCache;
import io.prestosql.queryeditorui.metadata.PreviewTableCache;
import io.prestosql.queryeditorui.metadata.SchemaCache;
import io.prestosql.queryeditorui.protocol.CatalogSchema;
import io.prestosql.queryeditorui.protocol.Table;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Path("/api/table")
public class TablesResource
{
    private final SchemaCache schemaCache;
    private final ColumnCache columnCache;
    private final PreviewTableCache previewTableCache;
    private final String defaultCatalog;
    private final CatalogManager catalogManager;
    private final DataCenterConnectorManager dataCenterConnectorManager;

    @Inject
    public TablesResource(
            final SchemaCache schemaCache,
            final ColumnCache columnCache,
            final PreviewTableCache previewTableCache,
            final CatalogManager catalogManager,
            final DataCenterConnectorManager dataCenterConnectorManager,
            @Named("default-catalog") final String defaultCatalog)
    {
        this.catalogManager = catalogManager;
        this.dataCenterConnectorManager = dataCenterConnectorManager;
        this.schemaCache = schemaCache;
        this.columnCache = columnCache;
        this.previewTableCache = previewTableCache;
        this.defaultCatalog = defaultCatalog;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTableUpdates(
            @QueryParam("catalog") String catalogName)
    {
        final List<String> catalogsToList = (null != catalogName && !catalogName.isEmpty()) ? ImmutableList.of(catalogName) :
                catalogManager.getCatalogs().stream().map(c -> c.getCatalogName()).collect(Collectors.toList());
        final ImmutableList.Builder<Table> builder = ImmutableList.builder();
        for (String catalog : catalogsToList) {
            final Map<String, List<String>> schemaMap = schemaCache.getSchemaMap(catalog, false);

            for (Map.Entry<String, List<String>> entry : schemaMap.entrySet()) {
                String schema = entry.getKey();
                for (String table : entry.getValue()) {
//                if (isAuthorizedRead(user, catalog, schema, table)) {
                    builder.add(new Table(catalog, schema, table));
//                }
                }
            }
        }

        final List<Table> tables = builder.build();
//        final Map<Table, Long> allUsages = usageStore.getUsages(tables);
//        final Map<PartitionedTable, DateTime> updateMap = Collections.emptyMap();

        return Response.ok(tables).build();
    }

    // TODO: Make getTableColumns, getTablePartitions and getTablePreview take a 3rd path parameter for catalog
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{schema}/{tableName}/columns")
    public Response getTableColumns(
            @PathParam("schema") String schema,
            @PathParam("tableName") String tableName)
            throws ExecutionException
    {
        List<Column> columnList = columnCache.getColumns(schema, tableName);
        return Response.ok(columnList).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{schema}/{tableName}/preview")
    public Response getTablePreview(
            @PathParam("schema") String schema,
            @PathParam("tableName") String tableName)
            throws ExecutionException
    {
//        if (isAuthorizedRead(user, defaultCatalog, schema, tableName)) {
        final List<List<Object>> preview = previewTableCache.getPreview(schema, tableName);
        return Response.ok(preview).build();
//        }
//        else {
//            return Response.status(Response.Status.FORBIDDEN).build();
//        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("schemas")
    public Response getCatalogs(
            @QueryParam("force") boolean force)
            throws ExecutionException
    {
        dataCenterConnectorManager.loadAllDCCatalogs();
        List<String> catalogs = catalogManager.getCatalogs().stream().map(c -> c.getCatalogName()).collect(Collectors.toList());
        final ImmutableList.Builder<CatalogSchema> builder = ImmutableList.builder();
        for (String catalog : catalogs) {
            if (force) {
                schemaCache.refreshCache(catalog);
            }
            List<String> schemas = schemaCache.getSchemasForCatalog(catalog);
            builder.add(new CatalogSchema(catalog, ImmutableList.copyOf(schemas)));
        }
        return Response.ok(builder.build()).build();
    }
}
