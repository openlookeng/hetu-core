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
package io.hetu.core.plugin.mpp;

import io.airlift.configuration.Config;

public class MppConfig
{
    private String gdsList;
    private boolean etlReuse;
    private String baseAux;
    private String auxUrl;

    private String hiveDb;
    private String hiveUrl;
    private String hiveUser;
    private String hivePasswd;
    private String hsqlDrop;
    private String hsqlCreate;

    private String gsDriver;
    private String gsUrl;
    private String gsUser;
    private String gsPasswd;

    private String gsqlCreate;
    private String gsqlInsert;
    private String gsqlDrop;

    public String getGdsList()
    {
        return gdsList;
    }

    @Config("gds-list")
    public MppConfig setGdsList(String gdsList)
    {
        this.gdsList = gdsList;
        return this;
    }

    public boolean isEtlReuse()
    {
        return etlReuse;
    }

    @Config("etl-reuse")
    public void setEtlReuse(boolean etlReuse)
    {
        this.etlReuse = etlReuse;
    }

    public String getGsDriver()
    {
        return gsDriver;
    }

    @Config("gs-driver")
    public void setGsDriver(String gsDriver)
    {
        this.gsDriver = gsDriver;
    }

    public String getGsUrl()
    {
        return gsUrl;
    }

    @Config("gs-url")
    public void setGsUrl(String gsUrl)
    {
        this.gsUrl = gsUrl;
    }

    public String getGsUser()
    {
        return gsUser;
    }

    @Config("gs-user")
    public void setGsUser(String gsUser)
    {
        this.gsUser = gsUser;
    }

    public String getGsPasswd()
    {
        return gsPasswd;
    }

    @Config("gs-passwd")
    public void setGsPasswd(String gsPasswd)
    {
        this.gsPasswd = gsPasswd;
    }

    public String getGsqlCreate()
    {
        return gsqlCreate;
    }

    @Config("gsql-create")
    public void setGsqlCreate(String gsqlCreate)
    {
        this.gsqlCreate = gsqlCreate;
    }

    public String getGsqlInsert()
    {
        return gsqlInsert;
    }

    @Config("gsql-insert")
    public void setGsqlInsert(String gsqlInsert)
    {
        this.gsqlInsert = gsqlInsert;
    }

    public String getGsqlDrop()
    {
        return gsqlDrop;
    }

    @Config("gsql-drop")
    public void setGsqlDrop(String gsqlDrop)
    {
        this.gsqlDrop = gsqlDrop;
    }

    public String getBaseAux()
    {
        return baseAux;
    }

    @Config("base-aux")
    public void setBaseAux(String baseAux)
    {
        this.baseAux = baseAux;
    }

    public String getAuxUrl()
    {
        return auxUrl;
    }

    @Config("aux-url")
    public void setAuxUrl(String auxUrl)
    {
        this.auxUrl = auxUrl;
    }

    public String getHiveDb()
    {
        return hiveDb;
    }

    @Config("hive-db")
    public void setHiveDb(String hiveDb)
    {
        this.hiveDb = hiveDb;
    }

    public String getHiveUrl()
    {
        return hiveUrl;
    }

    @Config("hive-url")
    public void setHiveUrl(String hiveUrl)
    {
        this.hiveUrl = hiveUrl;
    }

    public String getHiveUser()
    {
        return hiveUser;
    }

    @Config("hive-user")
    public void setHiveUser(String hiveUser)
    {
        this.hiveUser = hiveUser;
    }

    public String getHivePasswd()
    {
        return hivePasswd;
    }

    @Config("hive-passwd")
    public void setHivePasswd(String hivePasswd)
    {
        this.hivePasswd = hivePasswd;
    }

    public String getHsqlDrop()
    {
        return hsqlDrop;
    }

    @Config("hsql-drop")
    public void setHsqlDrop(String hsqlDrop)
    {
        this.hsqlDrop = hsqlDrop;
    }

    public String getHsqlCreate()
    {
        return hsqlCreate;
    }

    @Config("hsql-create")
    public void setHsqlCreate(String hsqlCreate)
    {
        this.hsqlCreate = hsqlCreate;
    }
}
