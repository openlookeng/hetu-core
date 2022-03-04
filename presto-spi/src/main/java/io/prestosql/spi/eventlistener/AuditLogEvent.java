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

package io.prestosql.spi.eventlistener;

import static java.util.Objects.requireNonNull;

public class AuditLogEvent
{
    private String user;

    private String ip;

    private String operation;

    private String type;

    private String level;

    public AuditLogEvent()
    {
    }

    public String getUser()
    {
        return user;
    }

    public String getIp()
    {
        return ip;
    }

    public String getOperation()
    {
        return operation;
    }

    public String getType()
    {
        return type;
    }

    public String getLevel()
    {
        return level;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setIp(String ip)
    {
        this.ip = ip;
    }

    public void setOperation(String operation)
    {
        this.operation = operation;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public void setLevel(String level)
    {
        this.level = level;
    }

    public AuditLogEvent(String user, String ip, String operation, String type, String level)
    {
        requireNonNull(user, "user is null");
        requireNonNull(ip, "ip is null");
        requireNonNull(operation, "operation is null");
        requireNonNull(type, "type is null");
        requireNonNull(level, "level is null");
        this.user = user;
        this.ip = ip;
        this.operation = operation;
        this.type = type;
        this.level = level;
    }

    @Override
    public String toString()
    {
        return "AuditLogEvent{" +
                "user='" + user + '\'' +
                ", ip='" + ip + '\'' +
                ", operation='" + operation + '\'' +
                '}';
    }
}
