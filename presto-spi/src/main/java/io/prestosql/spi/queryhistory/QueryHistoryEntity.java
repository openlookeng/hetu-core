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
package io.prestosql.spi.queryhistory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryHistoryEntity
{
    @JsonProperty
    private String queryId;
    @JsonProperty
    private String state;
    @JsonProperty
    private String failed;
    @JsonProperty
    private String query;
    @JsonProperty
    private String user;
    @JsonProperty
    private String source;
    @JsonProperty
    private String resource;
    @JsonProperty
    private String catalog;
    @JsonProperty
    private String schemata;
    @JsonProperty
    private String currentMemory;
    @JsonProperty
    private int completedDrivers;
    @JsonProperty
    private int runningDrivers;
    @JsonProperty
    private int queuedDrivers;
    @JsonProperty
    private String createTime;
    @JsonProperty
    private String executionTime;
    @JsonProperty
    private String elapsedTime;
    @JsonProperty
    private String cpuTime;
    @JsonProperty
    private String totalCpuTime;
    @JsonProperty
    private String totalMemoryReservation;
    @JsonProperty
    private String peakTotalMemoryReservation;
    @JsonProperty
    private double cumulativeUserMemory;

    @JsonCreator
    public QueryHistoryEntity()
    {
    }

    @JsonCreator
    public QueryHistoryEntity(
            String queryId,
            String state,
            String failed,
            String query,
            String user,
            String source,
            String resource,
            String catalog,
            String schemata,
            String currentMemory,
            int completedDrivers,
            int runningDrivers,
            int queuedDrivers,
            String createTime,
            String executionTime,
            String elapsedTime,
            String cpuTime,
            String totalCpuTime,
            String totalMemoryReservation,
            String peakTotalMemoryReservation,
            double cumulativeUserMemory)
    {
        this.queryId = queryId;
        this.state = state;
        this.failed = failed;
        this.query = query;
        this.user = user;
        this.source = source;
        this.resource = resource;
        this.catalog = catalog;
        this.schemata = schemata;
        this.currentMemory = currentMemory;
        this.completedDrivers = completedDrivers;
        this.runningDrivers = runningDrivers;
        this.queuedDrivers = queuedDrivers;
        this.createTime = createTime;
        this.executionTime = executionTime;
        this.elapsedTime = elapsedTime;
        this.cpuTime = cpuTime;
        this.totalCpuTime = totalCpuTime;
        this.totalMemoryReservation = totalMemoryReservation;
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
        this.cumulativeUserMemory = cumulativeUserMemory;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public String getFailed()
    {
        return failed;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public String getResource()
    {
        return resource;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchemata()
    {
        return schemata;
    }

    @JsonProperty
    public String getCurrentMemory()
    {
        return currentMemory;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public String getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public String getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public String getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public String getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public String getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public String getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public String getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    public void setQueryId(String queryId)
    {
        this.queryId = queryId;
    }

    public void setState(String state)
    {
        this.state = state;
    }

    public void setFailed(String failed)
    {
        this.failed = failed;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setSource(String source)
    {
        this.source = source;
    }

    public void setResource(String resource)
    {
        this.resource = resource;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    public void setSchemata(String schemata)
    {
        this.schemata = schemata;
    }

    public void setCurrentMemory(String currentMemory)
    {
        this.currentMemory = currentMemory;
    }

    public void setCompletedDrivers(int completedDrivers)
    {
        this.completedDrivers = completedDrivers;
    }

    public void setRunningDrivers(int runningDrivers)
    {
        this.runningDrivers = runningDrivers;
    }

    public void setQueuedDrivers(int queuedDrivers)
    {
        this.queuedDrivers = queuedDrivers;
    }

    public void setCreateTime(String createTime)
    {
        this.createTime = createTime;
    }

    public void setExecutionTime(String executionTime)
    {
        this.executionTime = executionTime;
    }

    public void setElapsedTime(String elapsedTime)
    {
        this.elapsedTime = elapsedTime;
    }

    public void setCpuTime(String cpuTime)
    {
        this.cpuTime = cpuTime;
    }

    public void setTotalCpuTime(String totalCpuTime)
    {
        this.totalCpuTime = totalCpuTime;
    }

    public void setTotalMemoryReservation(String totalMemoryReservation)
    {
        this.totalMemoryReservation = totalMemoryReservation;
    }

    public void setPeakTotalMemoryReservation(String peakTotalMemoryReservation)
    {
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
    }

    public void setCumulativeUserMemory(double cumulativeUserMemory)
    {
        this.cumulativeUserMemory = cumulativeUserMemory;
    }
}
