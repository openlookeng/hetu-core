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
package io.hetu.core.eventlistener.util;

import io.hetu.core.eventlistener.HetuEventListenerConfig;
import io.prestosql.spi.PrestoException;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class HetuLogUtil
{
    private static String logConversionPattern;

    private static String logOutput;

    private static ConcurrentHashMap<String, Logger> loggerConcurrentHashMap;

    private HetuLogUtil(){}

    public static void create(HetuEventListenerConfig config)
    {
        logConversionPattern = config.getAuditLogConversionPattern();
        logOutput = config.getAuditLogFile();
        loggerConcurrentHashMap = new ConcurrentHashMap<>();
        createFile(config.getAuditLogFile());
    }

    private static String getCurrentDate(String logConversionPattern)
    {
        String hetuLogConversionPattern = logConversionPattern;
        if (hetuLogConversionPattern == null) {
            hetuLogConversionPattern = "yyyy-MM-dd.HH";
        }
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter format = DateTimeFormatter.ofPattern(hetuLogConversionPattern);
        return now.format(format);
    }

    public static Logger getLoggerByName(String username, String level, AuditType type)
    {
        String fileName = logOutput + "/" + type + "/" + level + "#" + username + "#" + getCurrentDate(logConversionPattern) + ".log";
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(fileName);
        if (logOutput == null) {
            return logger;
        }
        try {
            if (loggerConcurrentHashMap.containsKey(fileName)) {
                return loggerConcurrentHashMap.get(fileName);
            }
            synchronized (HetuLogUtil.class) {
                if (loggerConcurrentHashMap.containsKey(fileName)) {
                    return loggerConcurrentHashMap.get(fileName);
                }
                FileHandler fileHandler = new FileHandler(fileName, 0, 1, true);
                fileHandler.setFormatter(new Formatter(){
                    @Override
                    public String format(LogRecord logRecord)
                    {
                        return logRecord.getMessage() + "\n";
                    }
                });
                logger.addHandler(fileHandler);
                logger.setUseParentHandlers(false);
                loggerConcurrentHashMap.put(fileName, logger);
                clearUnusedLogger();
            }
            return loggerConcurrentHashMap.get(fileName);
        }
        catch (IOException ex) {
            throw new PrestoException(ListenerErrorCode.LOCAL_FILE_FILESYSTEM_ERROR,
                    "failed to create logger writing to " + fileName, ex);
        }
    }

    private static void clearUnusedLogger()
    {
        String currentTime = getCurrentDate(logConversionPattern);
        for (String key : loggerConcurrentHashMap.keySet()) {
            String loggerTime = key.substring(key.lastIndexOf("#") + 1, key.length() - 4);
            if (loggerTime.compareTo(currentTime) < 0) {
                Logger deadLogger = loggerConcurrentHashMap.get(key);
                for (Handler h : deadLogger.getHandlers()) {
                    h.close();
                }
                loggerConcurrentHashMap.remove(key);
            }
        }
    }

    private static void createFile(String auditLogFile)
    {
        if (auditLogFile == null) {
            return;
        }
        File logFileSql = new File(auditLogFile + "/Sql");
        File logFileWebUi = new File(auditLogFile + "/WebUi");
        File logFileCluster = new File(auditLogFile + "/Cluster");
        logFileSql.mkdir();
        logFileWebUi.mkdir();
        logFileCluster.mkdir();
    }

    public enum AuditType{
        WebUi,
        Sql,
        Cluster
    }
}
