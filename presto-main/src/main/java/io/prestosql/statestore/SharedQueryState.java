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
package io.prestosql.statestore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.SessionRepresentation;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.ErrorCode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Optional;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

/**
 * Query state that can be serialized and externalized to external state store
 * and can be shared among all the coordinators in the cluster
 *
 * @since 2019-11-29
 */
public class SharedQueryState
{
    private final BasicQueryInfo basicQueryInfo;
    private final Optional<ErrorCode> errorCode;
    private final Duration totalCpuTime;
    private final DataSize totalMemoryReservation;
    private final DataSize userMemoryReservation;
    private final DateTime stateUpdateTime;
    private final Optional<DateTime> executionStartTime;

    @JsonCreator
    public SharedQueryState(
            @JsonProperty("basicQueryInfo") BasicQueryInfo basicQueryInfo,
            @JsonProperty("errorCode") Optional<ErrorCode> errorCode,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("stateUpdateTime") DateTime stateUpdateTime,
            @JsonProperty("executionStartTime") Optional<DateTime> executionStartTime)
    {
        this.basicQueryInfo = basicQueryInfo;
        this.errorCode = errorCode;
        this.userMemoryReservation = userMemoryReservation;
        this.totalMemoryReservation = totalMemoryReservation;
        this.totalCpuTime = totalCpuTime;
        this.stateUpdateTime = stateUpdateTime;
        this.executionStartTime = executionStartTime;
    }

    /**
     * Using query to create a SharedQueryState object
     *
     * @param query the query in this shared state object
     * @return a wrapped SharedQueryState object based on the query
     */
    public static SharedQueryState create(DispatchQuery query)
    {
        requireNonNull(query, "query is null");

        return new SharedQueryState(
                query.getBasicQueryInfo(),
                query.getErrorCode(),
                query.getUserMemoryReservation(),
                query.getTotalMemoryReservation(),
                query.getTotalCpuTime(),
                new DateTime(DateTimeZone.UTC),
                query.getExecutionStartTime());
    }

    @JsonProperty
    public BasicQueryInfo getBasicQueryInfo()
    {
        return basicQueryInfo;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return basicQueryInfo.getSession();
    }

    @JsonProperty
    public Optional<ErrorCode> getErrorCode()
    {
        return errorCode;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public DateTime getStateUpdateTime()
    {
        return stateUpdateTime;
    }

    @JsonProperty
    public Optional<DateTime> getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public OptionalDouble getQueryProgress()
    {
        return basicQueryInfo.getQueryStats().getProgressPercentage();
    }
}
