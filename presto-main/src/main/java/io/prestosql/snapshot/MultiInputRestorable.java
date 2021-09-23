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
package io.prestosql.snapshot;

import io.prestosql.spi.snapshot.Restorable;

import java.util.Optional;
import java.util.Set;

/**
 * A restorable object that may receive inputs from multiple channels
 */
public interface MultiInputRestorable
        extends Restorable
{
    /**
     * Retrieve all input channel identifiers
     *
     * @return A set of channel identifiers; empty if it cannot be determined at this point
     */
    Optional<Set<String>> getInputChannels();

    @Override
    default boolean supportsConsolidatedWrites()
    {
        // Multi-input restorable objects may potentially store a large number of pages
        // as part of their states. It's safer to not include these in the consolidated state file.
        return false;
    }
}
