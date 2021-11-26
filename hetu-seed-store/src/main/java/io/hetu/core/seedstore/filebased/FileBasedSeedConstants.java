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

package io.hetu.core.seedstore.filebased;

/**
 * Class that holds constant values for file-based seed store
 *
 * @since 2020-03-06
 */
public class FileBasedSeedConstants
{
    // Hazelcast seed file name
    static final String SEED_FILE_NAME = "seeds.txt";

    // ON-YARN seed file name
    static final String ON_YARN_SEED_FILE_NAME = "seeds-resources.json";

    // dir config properties name
    static final String SEED_STORE_FILESYSTEM_DIR = "seed-store.filesystem.seed-dir";

    // default seed store directory
    static final String SEED_STORE_FILESYSTEM_DIR_DEFAULT_VALUE = "/opt/hetu/seedstore";

    private FileBasedSeedConstants()
    {
    }
}
