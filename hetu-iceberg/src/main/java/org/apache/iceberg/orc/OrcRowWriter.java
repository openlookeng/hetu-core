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
package org.apache.iceberg.orc;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.FieldMetrics;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * Write data value of a schema.
 */
public interface OrcRowWriter<T>
{
    /**
     * Writes or appends a row to ORC's VectorizedRowBatch.
     *
     * @param row    the row data value to write.
     * @param output the VectorizedRowBatch to which the output will be written.
     * @throws IOException if there's any IO error while writing the data value.
     */
    void write(T row, VectorizedRowBatch output) throws IOException;

    List<OrcValueWriter<?>> writers();

    /**
     * Returns a stream of {@link FieldMetrics} that this OrcRowWriter keeps track of.
     */
    default Stream<FieldMetrics<?>> metrics()
    {
        return Stream.empty();
    }
}
