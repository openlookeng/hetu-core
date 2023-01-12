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
package io.prestosql.plugin.hive;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class PrestoFileStatusRemoteIterator
        implements RemoteIterator<PrestoFileStatus>
{
    private final RemoteIterator<LocatedFileStatus> iterator;

    public PrestoFileStatusRemoteIterator(RemoteIterator<LocatedFileStatus> iterator)
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        return iterator.hasNext();
    }

    @Override
    public PrestoFileStatus next()
            throws IOException
    {
        return new PrestoFileStatus(iterator.next());
    }
}
