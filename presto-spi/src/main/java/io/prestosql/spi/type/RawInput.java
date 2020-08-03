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

package io.prestosql.spi.type;

/**
 * This is a wrapper to encapsulate the differences between data source. HTTP, HDFS can use InputStream, JDBC can use
 * ResultSet, etc.
 *
 * <T> the type such as long, int, string of the data stored in the raw input
 */
public class RawInput<T>
{
    private Object input;

    RawInput(Object input)
    {
        this.input = input;
    }

    public Object get()
    {
        return input;
    }
}
