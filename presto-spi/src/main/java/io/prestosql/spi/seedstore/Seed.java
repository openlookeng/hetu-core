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
package io.prestosql.spi.seedstore;

import java.io.IOException;
import java.io.Serializable;

/**
 * Seed that contains information of a seed node
 *
 * @since 2020-03-04
 */
public interface Seed
        extends Serializable
{
    /**
     * Constant property name for seed node location
     */
    String LOCATION_PROPERTY_NAME = "location";

    /**
     * Constant property name for seed node info timestamp
     */
    String TIMESTAMP_PROPERTY_NAME = "timestamp";

    /**
     * Get location of seed
     *
     * @return location of seed
     */
    String getLocation();

    /**
     * Get timestamp of seed
     *
     * @return timestamp of seed
     */
    long getTimestamp();

    /**
     * Serialize seed object to string and return
     *
     * @return serialized string
     * @throws IOException exception when failed to serialize
     */
    String serialize()
            throws IOException;
}
