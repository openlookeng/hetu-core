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
package io.prestosql.testing;

import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NoOpIndexClient
        implements IndexClient
{
    private static final String ERROR_MSG = "Heuristic Index is not enabled or is configured incorrectly.";

    @Override
    public List<IndexMetadata> readSplitIndex(String path)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public long getLastModified(String path)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public List<IndexMetadata> readPartitionIndex(String path)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public void addIndexRecord(CreateIndexMetadata createIndexMetadata)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public void deleteIndexRecord(String indexName, List<String> partitionsToDelete)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public RecordStatus lookUpIndexRecord(CreateIndexMetadata createIndexMetadata)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public void deleteIndex(String indexName, List<String> partitionsToDelete)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }

    @Override
    public List<IndexRecord> getAllIndexRecords()
            throws IOException
    {
        return new ArrayList<IndexRecord>();
    }

    @Override
    public IndexRecord lookUpIndexRecord(String name)
            throws IOException
    {
        throw new UnsupportedOperationException(ERROR_MSG);
    }
}
