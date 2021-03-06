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

import com.google.inject.Inject;
import com.opencsv.CSVReader;
import io.prestosql.queryeditorui.store.files.ExpiringFileStore;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AccessControlUtil;
import io.prestosql.server.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Path("/api/preview")
public class ResultsPreviewResource
{
    private static final Logger LOG = LoggerFactory.getLogger(ResultsPreviewResource.class);
    private final ExpiringFileStore fileStore;
    private final AccessControl accessControl;
    private final ServerConfig serverConfig;

    @Inject
    public ResultsPreviewResource(ExpiringFileStore fileStore, AccessControl accessControl, ServerConfig serverConfig)
    {
        this.fileStore = fileStore;
        this.accessControl = accessControl;
        this.serverConfig = serverConfig;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFile(
            @QueryParam("pageNum") Integer pageNum,
            @QueryParam("pageSize") Integer pageSize,
            @QueryParam("fileURI") URI fileURI,
            @Context HttpServletRequest servletRequest)
    {
        // if the user is admin, don't filter results by user.
        Optional<String> filterUser = AccessControlUtil.getUserForFilter(accessControl, serverConfig, servletRequest);

        return getFilePreview(fileURI, pageNum, pageSize, filterUser);
    }

    private String getFilename(URI fileURI)
    {
        return fileURI.getPath().substring(fileURI.getPath().lastIndexOf('/') + 1);
    }

    private Response getPreviewFromCSV(CSVReader reader, Integer pageNum, Integer pageSize, String fileName)
    {
        List<Map<String, String>> columns = new ArrayList<>();
        List<List<String>> data = new ArrayList<>();
        try {
            for (final String columnName : reader.readNext()) {
                HashMap<String, String> columnsMap = new HashMap<>();
                columnsMap.put("name", columnName);
                columns.add(columnsMap);
            }
            for (String[] line : reader) {
                data.add(Arrays.asList(line));
            }
            int total = data.toArray().length;

            if (pageNum == null || pageSize == null || total == 0) {
                return Response.ok(new PreviewResponse(columns, data, total, fileName)).build();
            }
            if (pageNum <= 0 || pageSize <= 0) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            else if (total - pageSize * (pageNum - 1) <= 0) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            int start = (pageNum - 1) * pageSize;
            int end = Math.min(pageNum * pageSize, total);
            List<List<String>> subData = data.subList(start, end);
            return Response.ok(new PreviewResponse(columns, subData, total, fileName)).build();
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    private Response getFilePreview(URI fileURI, Integer pageNum, Integer pageSize, Optional<String> user)
    {
        String fileName = getFilename(fileURI);
        final File file = fileStore.get(fileName, user);
        try {
            if (file == null) {
                throw new FileNotFoundException(fileName + " could not be found");
            }
            else {
                try (final CSVReader reader = new CSVReader(new FileReader(file))) {
                    return getPreviewFromCSV(reader, pageNum, pageSize, fileName);
                }
                catch (IOException e) {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                }
            }
        }
        catch (FileNotFoundException e) {
            LOG.warn(e.getMessage());
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
