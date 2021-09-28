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

package io.prestosql.operator;

import java.net.URI;

public class TaskLocation
{
    private final URI uri;
    private final String instanceId;

    public TaskLocation(URI uri, String instanceId)
    {
        this.uri = uri;
        this.instanceId = instanceId;
    }

    public URI getUri()
    {
        return uri;
    }

    public String getInstanceId()
    {
        return instanceId;
    }
}
