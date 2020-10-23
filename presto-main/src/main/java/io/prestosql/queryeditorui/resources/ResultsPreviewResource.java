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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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

@Path("/api/preview")
public class ResultsPreviewResource
{
    private static final Logger LOG = LoggerFactory.getLogger(ResultsPreviewResource.class);
    private final ExpiringFileStore fileStore;

    @Inject
    public ResultsPreviewResource(ExpiringFileStore fileStore)
    {
        this.fileStore = fileStore;
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFile(@QueryParam("fileURI") URI fileURI,
                            @DefaultValue("100") @QueryParam("lines") int numLines)
    {
        return getFilePreview(fileURI, numLines);
    }

    private String getFilename(URI fileURI)
    {
        return fileURI.getPath().substring(fileURI.getPath().lastIndexOf('/') + 1);
    }

    private Response getPreviewFromCSV(CSVReader reader, final int numLines)
    {
        List<Map<String, String>> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        try {
            for (final String columnName : reader.readNext()) {
                HashMap<String, String> columnsMap = new HashMap<String, String>();
                columnsMap.put("name", columnName);
                columns.add(columnsMap);
            }
            int counter = 0;
            for (String[] line : reader) {
                counter++;
                rows.add(Arrays.asList(line));
                if (counter >= numLines) {
                    break;
                }
            }
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok(new PreviewResponse(columns, rows)).build();
    }

    private Response getFilePreview(URI fileURI, int numLines)
    {
        String fileName = getFilename(fileURI);
        final File file = fileStore.get(fileName);
        try {
            if (file == null) {
                throw new FileNotFoundException(fileName + " could not be found");
            }
            try (final CSVReader reader = new CSVReader(new FileReader(file))) {
                return getPreviewFromCSV(reader, numLines);
            }
            catch (IOException e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }
        }
        catch (FileNotFoundException e) {
            LOG.warn(e.getMessage());
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
