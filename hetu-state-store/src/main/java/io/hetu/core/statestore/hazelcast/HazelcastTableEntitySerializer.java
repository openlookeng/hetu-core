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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.prestosql.spi.metastore.model.TableEntity;

import java.io.IOException;

import static io.hetu.core.statestore.hazelcast.HazelCastSerializationConstants.CONSTANT_TYPE_TABLEENTITY;

public class HazelcastTableEntitySerializer
        implements StreamSerializer<TableEntity>
{
    private ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

    @Override
    public void write(ObjectDataOutput objectDataOutput, TableEntity tableEntity)
            throws IOException
    {
        objectDataOutput.writeByteArray(mapper.writeValueAsString(tableEntity).getBytes());
    }

    @Override
    public TableEntity read(ObjectDataInput objectDataInput)
            throws IOException
    {
        return mapper.readValue(objectDataInput.readByteArray(), TableEntity.class);
    }

    @Override
    public int getTypeId()
    {
        return CONSTANT_TYPE_TABLEENTITY;
    }
}
