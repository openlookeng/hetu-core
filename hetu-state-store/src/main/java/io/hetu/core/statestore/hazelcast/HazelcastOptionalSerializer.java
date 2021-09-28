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
package io.hetu.core.statestore.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Optional;

import static io.hetu.core.statestore.hazelcast.HazelCastSerializationConstants.CONSTANT_TYPE_OPTIONAL;

public class HazelcastOptionalSerializer
        implements StreamSerializer<Optional>
{
    @Override
    public void write(ObjectDataOutput out, Optional optional)
            throws IOException
    {
        if (optional.isPresent()) {
            out.writeBoolean(true);
            out.writeObject(optional.get());
        }
        else {
            out.writeBoolean(false);
        }
    }

    @Override
    public Optional read(ObjectDataInput in)
            throws IOException
    {
        final boolean present = in.readBoolean();
        if (present) {
            return Optional.of(in.readObject());
        }
        return Optional.empty();
    }

    @Override
    public int getTypeId()
    {
        return CONSTANT_TYPE_OPTIONAL;
    }
}
