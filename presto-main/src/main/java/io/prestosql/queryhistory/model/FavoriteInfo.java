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

public class FavoriteInfo
{
    @JsonProperty
    private String user;

    @JsonProperty
    private String query;

    @JsonProperty
    private String catalog;

    @JsonProperty
    private String schemata;

    @JsonCreator
    public FavoriteInfo()
    {
    }

    @JsonCreator
    public FavoriteInfo(@JsonProperty("user") String user,
                        @JsonProperty("query") String query,
                        @JsonProperty("catalog") String catalog,
                        @JsonProperty("schemata") String schemata)
    {
        this.user = user;
        this.query = query;
        this.catalog = catalog;
        this.schemata = schemata;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
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

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    public void setSchemata(String schemata)
    {
        this.schemata = schemata;
    }
}
