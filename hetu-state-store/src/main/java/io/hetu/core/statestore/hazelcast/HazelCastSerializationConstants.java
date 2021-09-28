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

public class HazelCastSerializationConstants
{
    public static final int CONSTANT_TYPE_SLICE = 1;
    public static final int CONSTANT_TYPE_OPTIONAL = 2;
    public static final int CONSTANT_TYPE_DATABASEENTITY = 3;
    public static final int CONSTANT_TYPE_TABLEENTITY = 4;
    public static final int CONSTANT_TYPE_CATALOG = 5;

    private HazelCastSerializationConstants()
    {
    }
}
