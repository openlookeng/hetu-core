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
package io.hetu.core.eventlistener;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.hetu.core.eventlistener.listeners.BaseEventListener;

import javax.validation.constraints.Min;

public class HetuEventListenerConfig
{
    private BaseEventListener.Type type;

    private boolean isListenQueryCreation;

    private boolean isListenQueryCompletion;

    private boolean isListenSplitCompletion;

    private String logFile;

    private int logFileLimit;

    private int logFileCount = 1;

    private String auditFile;

    private int auditFileLimit;

    private int auditFileCount = 1;

    public BaseEventListener.Type getType()
    {
        return type;
    }

    /**
     * set type
     *
     * @param typeParameter type from property file
     * @return config object
     */
    @Config("hetu.event.listener.type")
    @ConfigDescription("Hetu supports the following listeners: LOGGER, AUDIT. This property is case sensitive")
    public HetuEventListenerConfig setType(BaseEventListener.Type typeParameter)
    {
        this.type = typeParameter;
        return this;
    }

    public boolean isListenQueryCreation()
    {
        return isListenQueryCreation;
    }

    /**
     * set listenQueryCreation
     *
     * @param isListenQueryCreationParameter listenQueryCreation from property file
     * @return config object
     */
    @Config("hetu.event.listener.listen.query.creation")
    @ConfigDescription("Listen query creation events")
    public HetuEventListenerConfig setListenQueryCreation(boolean isListenQueryCreationParameter)
    {
        this.isListenQueryCreation = isListenQueryCreationParameter;
        return this;
    }

    public boolean isListenQueryCompletion()
    {
        return isListenQueryCompletion;
    }

    /**
     * set isListenQueryCompletion
     *
     * @param isListenQueryCompletionParameter isListenQueryCompletion from properties file
     * @return config object
     */
    @Config("hetu.event.listener.listen.query.completion")
    @ConfigDescription("Listen query completion events")
    public HetuEventListenerConfig setListenQueryCompletion(boolean isListenQueryCompletionParameter)
    {
        this.isListenQueryCompletion = isListenQueryCompletionParameter;
        return this;
    }

    public boolean isListenSplitCompletion()
    {
        return isListenSplitCompletion;
    }

    /**
     * set isListenSplitCompletion
     *
     * @param isListenSplitCompletionParameter isListenSplitCompletion from properties file
     * @return config object
     */
    @Config("hetu.event.listener.listen.split.completion")
    @ConfigDescription("Listen split completion events")
    public HetuEventListenerConfig setListenSplitCompletion(boolean isListenSplitCompletionParameter)
    {
        this.isListenSplitCompletion = isListenSplitCompletionParameter;
        return this;
    }

    public String getLogFile()
    {
        return logFile;
    }

    /**
     * set logFile
     *
     * @param logFile logFile from properties file
     * @return config object
     */
    @Config("hetu.event.listener.logger.file")
    @ConfigDescription(
            "Optional property to define absolute file path for the logger. If not set, logger will use the default "
                    + "log file")
    public HetuEventListenerConfig setLogFile(String logFile)
    {
        this.logFile = logFile;
        return this;
    }

    @Min(1)
    public int getLogFileCount()
    {
        return logFileCount;
    }

    /**
     * set longFileCount
     *
     * @param logFileCount logFileCount from properties file
     * @return config object
     */
    @Config("hetu.event.listener.logger.count")
    @ConfigDescription("Optional property to define the number of files to use")
    public HetuEventListenerConfig setLogFileCount(int logFileCount)
    {
        this.logFileCount = logFileCount;
        return this;
    }

    @Min(0)
    public int getLogFileLimit()
    {
        return logFileLimit;
    }

    /**
     * set logFileLimit
     *
     * @param logFileLimit logFileLimit from properties file
     * @return config object
     */
    @Config("hetu.event.listener.logger.limit")
    @ConfigDescription("Optional property to define the maximum number of bytes to write to any one file")
    public HetuEventListenerConfig setLogFileLimit(int logFileLimit)
    {
        this.logFileLimit = logFileLimit;
        return this;
    }

    public String getAuditFile()
    {
        return auditFile;
    }

    /**
     * set auditFile
     *
     * @param auditFile logFile from properties file
     * @return config object
     */
    @Config("hetu.event.listener.audit.file")
    @ConfigDescription(
            "Optional property to define absolute file path for the audit logger. If not set, audit logger will use the default "
                    + "log file")
    public HetuEventListenerConfig setAuditFile(String auditFile)
    {
        this.auditFile = auditFile;
        return this;
    }

    @Min(1)
    public int getAuditFileCount()
    {
        return auditFileCount;
    }

    /**
     * set auditFileCount
     *
     * @param auditFileCount auditFileCount from properties file
     * @return config object
     */
    @Config("hetu.event.listener.audit.filecount")
    @ConfigDescription("Optional property to define the number of files to use")
    public HetuEventListenerConfig setAuditFileCount(int auditFileCount)
    {
        this.auditFileCount = auditFileCount;
        return this;
    }

    @Min(0)
    public int getAuditFileLimit()
    {
        return auditFileLimit;
    }

    /**
     * set auditFileLimit
     *
     * @param auditFileLimit auditFileLimit from properties file
     * @return config object
     */
    @Config("hetu.event.listener.audit.limit")
    @ConfigDescription("Optional property to define the maximum number of bytes to write to any one file")
    public HetuEventListenerConfig setAuditFileLimit(int auditFileLimit)
    {
        this.auditFileLimit = auditFileLimit;
        return this;
    }
}
