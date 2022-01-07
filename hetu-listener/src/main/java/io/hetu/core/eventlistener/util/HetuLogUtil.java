/*
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
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Objects.requireNonNull;

public class HetuLogUtil
{
    private static String logConversionPattern;

    private static String logOutput;

    public HetuLogUtil(HetuEventListenerConfig config)
    {
        requireNonNull(config, "config is null");
        this.logConversionPattern = config.getAuditLogConversionPattern();
        this.logOutput = config.getAuditLogFile();
        this.createFile(config.getAuditLogFile());
    }

    private static String getCurrentDate(String logConversionPattern)
    {
        if (logConversionPattern == null) {
            logConversionPattern = "yyyy-MM-dd.HH";
        }
        SimpleDateFormat formatter = new SimpleDateFormat(logConversionPattern);
        String dateString = formatter.format(new Date());
        return dateString;
    }

    public static Logger getLoggerByName(String username, String level, AuditType type)
    {
        Logger logger = Logger.getLogger(username + Thread.currentThread().getName());
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        logger.setAdditivity(false);

        FileAppender appender = new FileAppender();
        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern("%d{yyyy-MM-dd HH:mm:ss} %p %C %m%n");

        appender.setLayout(layout);
        appender.setFile(logOutput + "/" + type + "/" + level + "#" + username + "#" + getCurrentDate(logConversionPattern) + ".log");
        appender.setEncoding("UTF-8");
        appender.setAppend(true);
        appender.activateOptions();

        logger.addAppender(appender);
        return logger;
    }

    private void createFile(String auditLogFile)
    {
        if (auditLogFile == null) {
            return;
        }
        System.setProperty("hetu-LogOutput", auditLogFile);
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
