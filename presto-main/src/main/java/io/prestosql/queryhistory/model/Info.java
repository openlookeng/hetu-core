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
package io.prestosql.queryhistory.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Info
{
    @JsonProperty
    private String user;

    @JsonProperty
    private String startTime;

    @JsonProperty
    private String endTime;

    @JsonProperty
    private String queryid;

    @JsonProperty
    private String query;

    @JsonProperty
    private String source;

    @JsonProperty
    private String resource;

    @JsonProperty
    private String state;

    @JsonProperty
    private String failed;

    @JsonProperty
    private String sort;

    @JsonProperty
    private String sortorder;

    @JsonCreator
    public Info(
            @JsonProperty("user") final String user,
            @JsonProperty("starttime") final String startTime,
            @JsonProperty("endtime") final String endTime,
            @JsonProperty("queryid") final String queryid,
            @JsonProperty("query") final String query,
            @JsonProperty("source") final String source,
            @JsonProperty("resource") final String resource,
            @JsonProperty("state") final String state,
            @JsonProperty("failed") final String failed,
            @JsonProperty("sort") final String sort,
            @JsonProperty("sortorder") final String sortorder)
    {
        this.user = user;
        this.startTime = startTime;
        this.endTime = endTime;
        this.queryid = queryid;
        this.query = query;
        this.source = source;
        this.resource = resource;
        this.state = state;
        this.failed = failed;
        this.sort = sort;
        this.sortorder = sortorder;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getUser()
    { return user; }

    @JsonProperty
    public String getQueryId()
    { return queryid; }

    @JsonProperty
    public String getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public String getEndTime()
    {
        return endTime;
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
    public String getSort()
    {
        return sort;
    }

    @JsonProperty
    public String getSortorder()
    {
        return sortorder;
    }
}
