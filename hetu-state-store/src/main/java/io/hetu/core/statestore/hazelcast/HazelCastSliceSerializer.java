/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.statestore.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.airlift.slice.Slice;

import java.io.IOException;

import static io.airlift.slice.Slices.wrappedBuffer;

public class HazelCastSliceSerializer
        implements StreamSerializer<Slice>
{
    @Override
    public void write(ObjectDataOutput objectDataOutput, Slice slice) throws IOException
    {
        objectDataOutput.writeByteArray(slice.getBytes());
    }

    @Override
    public Slice read(ObjectDataInput objectDataInput) throws IOException
    {
        return wrappedBuffer(objectDataInput.readByteArray());
    }

    @Override
    public int getTypeId()
    {
        return 1;
    }

    @Override
    public void destroy()
    {
    }
}
