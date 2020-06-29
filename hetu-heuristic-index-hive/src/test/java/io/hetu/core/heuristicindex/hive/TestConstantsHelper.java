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
package io.hetu.core.heuristicindex.hive;

public final class TestConstantsHelper
{
    private TestConstantsHelper() {}

    /**
     * Folder path for all test files
     */
    public static final String TEST_FOLDER_PATH = "src/test/resources/";

    /**
     * Folder path for all required config files to connect to the docker container
     */
    public static final String TEST_CONFIG_FOLDER_PATH = TEST_FOLDER_PATH + "docker_config/";

    /**
     * The HDFS hdfs-site file name from docker
     */
    public static final String DOCKER_HDFS_SITE_FILE = TEST_CONFIG_FOLDER_PATH + "hdfs-site.xml";

    /**
     * The HDFS core-site file name from docker
     */
    public static final String DOCKER_CORE_SITE_FILE = TEST_CONFIG_FOLDER_PATH + "core-site.xml";

    /**
     * hive.properties file from docker
     */
    public static final String DOCKER_HIVE_PROPERTIES_FILE_LOCATION = TEST_CONFIG_FOLDER_PATH + "/catalog/hive.properties";

    /**
     * kerberized hive.properties file from docker
     */
    public static final String DOCKER_KERBERIZED_HIVE_PROPERTIES_FILE_LOCATION = TEST_CONFIG_FOLDER_PATH + "hive-kerberized.properties";

    /**
     * Make sure this is the same as <code>HdfsOrcDataSource#DEFAULT_MAX_ROWS_PER_SPLIT</code>
     */
    public static final int DEFAULT_MAX_ROWS_PER_SPLIT = 200000;

    /**
     * Make sure this is the same as <code>HdfsOrcDataSource#DEFAULT_CONCURRENCY</code>
     */
    public static final int DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors();
}
