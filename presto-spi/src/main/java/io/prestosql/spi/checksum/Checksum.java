/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.spi.checksum;

/**
 * digest and verify checksum interface
 */
public interface Checksum
{
    /**
     * digest checksum based the byte array data
     *
     * @param data byte array data
     * @return checksum
     */
    String digest(byte[] data);

    /**
     * generate final result after a serial calls of update
     *
     * @return checksum value
     */
    String digest();

    /**
     * update using the data
     *
     * @param data data to digest
     */
    void update(byte[] data);

    /**
     * verify checksum of the data
     *
     * @param data     data to be verified
     * @param checksum original checksum of the data
     * @return true if the newly calculated checksum equals the original checksum
     */
    default boolean verify(byte[] data, String checksum)
    {
        return digest(data).equals(checksum);
    }
}
