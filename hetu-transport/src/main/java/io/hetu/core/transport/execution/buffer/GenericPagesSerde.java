/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.transport.execution.buffer;

import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;

public interface GenericPagesSerde
{
    default SerializedPage serialize(Page page)
    {
        throw new PrestoException(StandardErrorCode.NOT_FOUND, "Implementations for step serialization not found");
    }

    default Page deserialize(SerializedPage page)
    {
        throw new PrestoException(StandardErrorCode.NOT_FOUND, "Implementations for step deserialization not found");
    }

    /* interface to Serialize the page directly to the stream instead of creating serialized page and then writing it! */
    default void serialize(OutputStream output, Page page)
    {
        throw new PrestoException(StandardErrorCode.NOT_FOUND, "Implementations for step serialization not found");
    }

    /* interface to read the page directly from the stream instead of creating serialized page and then deserialize it! */
    default Page deserialize(InputStream input)
    {
        throw new PrestoException(StandardErrorCode.NOT_FOUND, "Implementations for step deserialization not found");
    }
}
