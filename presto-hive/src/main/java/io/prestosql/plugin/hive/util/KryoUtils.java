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
package io.prestosql.plugin.hive.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class KryoUtils
{
    private KryoUtils() {}

    public static byte[] serializeUsingKryo(Serializable object)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        Kryo kryo = new Kryo();
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        try {
            kryo.writeObject(output, object);
        }
        finally {
            kryo.reset();
        }
        output.close();
        return baos.toByteArray();
    }

    public static <T extends Serializable> T deserializeObjectFromKryo(byte[] bytes, Class<T> clazz)
    {
        Input input = new Input(new ByteArrayInputStream(bytes));
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        T object;
        try {
            object = kryo.readObject(input, clazz);
        }
        finally {
            kryo.reset();
        }
        input.close();
        return object;
    }
}
