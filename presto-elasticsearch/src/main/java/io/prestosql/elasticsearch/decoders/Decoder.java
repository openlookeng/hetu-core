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
package io.prestosql.elasticsearch.decoders;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;

import java.util.function.Supplier;

import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;

public interface Decoder
{
    void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output);

    default void decode(Aggregations aggregations, Supplier<Object> getter, BlockBuilder output)
    {
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, "Aggregation Decode not implemented yet");
    }
}
