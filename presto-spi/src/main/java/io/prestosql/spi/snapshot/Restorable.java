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
package io.prestosql.spi.snapshot;

/**
 * Indicate an object supports snapshotting: to capture and restore its internal states
 */
public interface Restorable
{
    /**
     * Capture this object's internal state, so it can be used later to restore to the same state.
     *
     * @param serdeProvider
     * @return An object representing internal state of the current object
     */
    default Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not support capture()");
    }

    /**
     * Restore this object's internal state according to the snapshot
     *
     * @param state an object that represents this object's snapshot state
     * @param serdeProvider
     */
    default void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not support restore()");
    }

    /**
     * Indicates if an object's internal state snapshot can be written into a consolidated file
     *
     * @return A boolean value representing whether or not this snapshot result can be consolidated
     */
    default boolean supportsConsolidatedWrites()
    {
        return true;
    }
}
