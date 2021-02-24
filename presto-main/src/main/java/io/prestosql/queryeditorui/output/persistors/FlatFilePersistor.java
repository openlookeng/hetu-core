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
package io.prestosql.queryeditorui.output.persistors;

import io.prestosql.queryeditorui.execution.QueryExecutionAuthorizer;
import io.prestosql.queryeditorui.output.builders.JobOutputBuilder;
import io.prestosql.queryeditorui.protocol.Job;
import io.prestosql.queryeditorui.store.files.ExpiringFileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static java.lang.String.format;

public class FlatFilePersistor
        implements Persistor
{
    private static final Logger LOG = LoggerFactory.getLogger(FlatFilePersistor.class);
    private final ExpiringFileStore fileStore;

    public FlatFilePersistor(ExpiringFileStore fileStore)
    {
        this.fileStore = fileStore;
    }

    @Override
    public boolean canPersist(QueryExecutionAuthorizer authorizer)
    {
        return true;
    }

    @Override
    public URI persist(JobOutputBuilder outputBuilder, Job job)
    {
        File file = outputBuilder.build();

        try {
            fileStore.addFile(file.getName(), job.getUser(), file);
        }
        catch (IOException e) {
            LOG.error("Caught error adding file to local store", e);
        }

        return URI.create(format("../api/files/%s", file.getName()));
    }
}
