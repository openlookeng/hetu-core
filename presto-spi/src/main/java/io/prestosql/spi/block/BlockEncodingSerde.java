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
package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;

public interface BlockEncodingSerde
{
    /**
     * Read a block encoding from the input.
     */
    default Block readBlock(InputStream input)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not supported");
    }

    /**
     * Write a blockEncoding to the output.
     */
    default void writeBlock(OutputStream output, Block block)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not supported");
    }

    /**
     * Read a block encoding from the input.
     */
    default Block readBlock(SliceInput input)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not supported");
    }

    /**
     * Write a blockEncoding to the output.
     */
    default void writeBlock(SliceOutput output, Block block)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not supported");
    }

    /* give out context object if any */
    default Object getContext()
    {
        return null;
    }
}
