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
package io.prestosql.queryeditorui.output.builders;

import io.prestosql.queryeditorui.execution.InvalidQueryException;
import io.prestosql.queryeditorui.output.PersistentJobOutput;
import io.prestosql.queryeditorui.protocol.Job;

import java.io.IOException;

import static java.lang.String.format;

public class OutputBuilderFactory
{
    private final long maxFileSizeBytes;
    private final boolean isCompressedOutput;

    public OutputBuilderFactory(long maxFileSizeBytes, boolean isCompressedOutput)
    {
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.isCompressedOutput = isCompressedOutput;
    }

    public JobOutputBuilder forJob(Job job)
            throws IOException, InvalidQueryException
    {
        PersistentJobOutput output = job.getOutput();
        switch (output.getType()) {
            case "csv":
                return new CsvOutputBuilder(true, job.getUuid(), maxFileSizeBytes, isCompressedOutput);
            default:
                throw new IllegalArgumentException(format("OutputBuilder for type %s not found", output.getType()));
        }
    }
}
