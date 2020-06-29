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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

public interface HiveFileWriter
{
    long getWrittenBytes();

    long getSystemMemoryUsage();

    void appendRows(Page dataPage);

    void commit();

    void rollback();

    long getValidationCpuNanos();

    default Optional<Runnable> getVerificationTask()
    {
        return Optional.empty();
    }

    default void initWriter(boolean isAcid, Path path, FileSystem fileSystem)
    {
    }

    default ImmutableList<String> getExtraPartitionFiles()
    {
        return ImmutableList.of();
    }

    default ImmutableList<String> getMiscData()
    {
        return ImmutableList.of();
    }
}
