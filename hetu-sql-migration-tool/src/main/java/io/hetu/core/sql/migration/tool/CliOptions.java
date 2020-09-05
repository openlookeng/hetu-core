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

package io.hetu.core.sql.migration.tool;

import io.airlift.airline.Option;

public class CliOptions
{
    @Option(name = {"-f", "--file"}, title = "file", description = "SQL file path")
    public String sqlFile;

    @Option(name = {"-t", "--type"}, title = "source type", description = "SQL type, eg. hive")
    public String sourceType;

    @Option(name = {"-e", "--execute"}, title = "execute", description = "Execute specified statements and exit")
    public String execute;

    @Option(name = {"-o", "--output"}, title = "output", description = "Output file path")
    public String outputPath;

    @Option(name = {"-c", "--config"}, title = "config", description = "Config file path")
    public String configFile;

    @Option(name = {"-d", "--debug"}, title = "debug", description = "Print Debug info")
    public String debug;
}
