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

import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryContext;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.QueryMetadata;
import io.prestosql.spi.eventlistener.QueryStatistics;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;
import io.prestosql.spi.eventlistener.SplitStatistics;

import java.util.StringJoiner;
import java.util.function.BiConsumer;

public class EventUtility
{
    /**
     * constant for tab
     */
    public static final String TAB = "\t";

    /**
     * constant for COLUMN
     */
    public static final String COLUMN = ": ";

    /**
     * constant for QUERY_ID
     */
    public static final String QUERY_ID = "Query ID";

    /**
     * constant for CREATE_TIME
     */
    public static final String CREATE_TIME = "Create Time";

    /**
     * constant for QUERY_FAILURE_TYPE
     */
    public static final String QUERY_FAILURE_TYPE = "Query Failure Type";

    private EventUtility()
    {
        // Utility class
    }

    /**
     * convert a query complete event to String
     *
     * @param event event object
     * @return string represents the event
     */
    public static String toString(QueryCompletedEvent event)
    {
        StringJoiner joiner = new StringJoiner(System.lineSeparator());
        joiner.add("---------------Query Completed----------------------------");
        visit(event, (key, value) -> {
            joiner.add(TAB + key + COLUMN + value);
        });
        return joiner.toString();
    }

    /**
     * convert a query created event to String
     *
     * @param event event object
     * @return string represents the event
     */
    public static String toString(QueryCreatedEvent event)
    {
        StringJoiner joiner = new StringJoiner(System.lineSeparator());
        joiner.add("---------------Query Created----------------------------");
        visit(event, (key, value) -> {
            joiner.add(TAB + key + COLUMN + value);
        });
        return joiner.toString();
    }

    /**
     * convert a split complete event to String
     *
     * @param event event object
     * @return string represents the event
     */
    public static String toString(SplitCompletedEvent event)
    {
        StringJoiner joiner = new StringJoiner(System.lineSeparator());
        joiner.add("---------------Split Completed----------------------------");
        visit(event, (key, value) -> {
            joiner.add(TAB + key + ": " + value);
        });
        return joiner.toString();
    }

    private static void visit(QueryCreatedEvent event, BiConsumer<String, Object> consumer)
    {
        QueryMetadata metadata = event.getMetadata();
        QueryContext context = event.getContext();
        consumer.accept(QUERY_ID, metadata.getQueryId());
        consumer.accept("Query", metadata.getQuery());
        consumer.accept(CREATE_TIME, event.getCreateTime());
        consumer.accept("User", context.getUser());
        context.getRemoteClientAddress().ifPresent(address -> consumer.accept("Remote Client Address", address));
        context.getCatalog().ifPresent(catalog -> consumer.accept("Catalog", catalog));
        context.getSchema().ifPresent(schema -> consumer.accept("Schema", schema));
    }

    private static void visit(QueryCompletedEvent event, BiConsumer<String, Object> consumer)
    {
        QueryMetadata metadata = event.getMetadata();
        QueryContext context = event.getContext();
        QueryStatistics statistics = event.getStatistics();
        consumer.accept(QUERY_ID, metadata.getQueryId());
        consumer.accept("Query", metadata.getQuery());
        consumer.accept(CREATE_TIME, event.getCreateTime());
        consumer.accept("Execution Start Time", event.getExecutionStartTime());
        consumer.accept("End Time", event.getEndTime());
        consumer.accept("Wall Time", statistics.getWallTime().toMillis());
        consumer.accept("User", context.getUser());
        consumer.accept("Complete", statistics.isComplete());
        context.getRemoteClientAddress().ifPresent(address -> consumer.accept("Remote Client Address", address));
        context.getCatalog().ifPresent(catalog -> consumer.accept("Catalog", catalog));
        context.getSchema().ifPresent(schema -> consumer.accept("Schema", schema));

        event.getFailureInfo().ifPresent(failureInfo -> {
            consumer.accept("Query Failure Error", failureInfo.getErrorCode());
            failureInfo.getFailureType().ifPresent(type -> consumer.accept(QUERY_FAILURE_TYPE, type));
            failureInfo.getFailureMessage().ifPresent(message -> consumer.accept(QUERY_FAILURE_TYPE, message));
            failureInfo.getFailureTask().ifPresent(task -> consumer.accept(QUERY_FAILURE_TYPE, task));
            failureInfo.getFailureHost().ifPresent(host -> consumer.accept(QUERY_FAILURE_TYPE, host));
        });
    }

    private static void visit(SplitCompletedEvent event, BiConsumer<String, Object> consumer)
    {
        consumer.accept(QUERY_ID, event.getQueryId());
        consumer.accept("Stage ID", event.getStageId());
        consumer.accept("Task ID", event.getTaskId());
        consumer.accept(CREATE_TIME, event.getCreateTime());
        event.getStartTime().ifPresent(startTime -> consumer.accept("Start Time", startTime));
        event.getEndTime().ifPresent(endTime -> consumer.accept("End Time", endTime));
        event.getFailureInfo().ifPresent(failureInfo -> {
            consumer.accept("Split Failure Type", failureInfo.getFailureType());
            consumer.accept("Split Failure Message", failureInfo.getFailureMessage());
        });
        SplitStatistics statistics = event.getStatistics();
        consumer.accept("CPU Time", statistics.getCpuTime());
        consumer.accept("Wall Time", statistics.getWallTime());
        consumer.accept("Queued Time", statistics.getQueuedTime());
        consumer.accept("Completed Positions", statistics.getCompletedPositions());
        consumer.accept("Completed Data Size", statistics.getCompletedDataSizeBytes());
    }
}
