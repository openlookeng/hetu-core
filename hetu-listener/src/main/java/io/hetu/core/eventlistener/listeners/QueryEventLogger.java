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
package io.hetu.core.eventlistener.listeners;

import io.airlift.log.Logger;
import io.hetu.core.eventlistener.HetuEventListenerConfig;
import io.hetu.core.eventlistener.util.EventUtility;
import io.hetu.core.eventlistener.util.ListenerErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

class QueryEventLogger
        extends BaseEventListener
{
    private final Logger logger;

    QueryEventLogger(HetuEventListenerConfig config)
    {
        super(config);
        if (config.getLogFile() != null) {
            // Airlift logger is using java.util.logging.Logger underneath.
            // Creating a java.util.logging.Logger using the same name will make the airlift logger to reuse it
            java.util.logging.Logger log = createLogger(Paths.get(config.getLogFile()), config.getLogFileLimit(),
                    config.getLogFileCount());
        }
        this.logger = Logger.get(QueryEventLogger.class);
    }

    private static java.util.logging.Logger createLogger(Path filePath, int limit, int count)
    {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(QueryEventLogger.class.getName());
        try {
            FileHandler fileHandler = new FileHandler(filePath.toAbsolutePath().toString(), limit, count, true);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setUseParentHandlers(false);
            return logger;
        }
        catch (IOException ex) {
            throw new PrestoException(ListenerErrorCode.LOCAL_FILE_FILESYSTEM_ERROR,
                    "failed to create logger writing to " + filePath.toAbsolutePath(), ex);
        }
    }

    @Override
    protected void onQueryCreatedEvent(QueryCreatedEvent queryCreatedEvent)
    {
        logger.info(EventUtility.toString(queryCreatedEvent));
    }

    @Override
    protected void onQueryCompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
        logger.info(EventUtility.toString(queryCompletedEvent));
    }

    @Override
    protected void onSplitCompletedEvent(SplitCompletedEvent splitCompletedEvent)
    {
        logger.info(EventUtility.toString(splitCompletedEvent));
    }
}
