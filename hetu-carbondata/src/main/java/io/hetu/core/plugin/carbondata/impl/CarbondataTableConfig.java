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
package io.hetu.core.plugin.carbondata.impl;

import io.airlift.configuration.Config;

/**
 * Configuration read from etc/catalog/carbondata.properties
 */
public class CarbondataTableConfig
{
    //read from config
    private String unsafeMemoryInMb;
    private String enableUnsafeInQueryExecution;
    private String enableUnsafeColumnPage;
    private String enableUnsafeSort;
    private String enableQueryStatistics;
    private String batchSize;
    private String s3AAcesssKey;
    private String s3ASecretKey;
    private String s3AcesssKey;
    private String s3SecretKey;
    private String s3NAcesssKey;
    private String s3NSecretKey;
    private String endPoint;
    private String pushRowFilter;

    public String getUnsafeMemoryInMb()
    {
        return unsafeMemoryInMb;
    }

    @Config("carbon.unsafe.working.memory.in.mb")
    public CarbondataTableConfig setUnsafeMemoryInMb(String unsafeMemoryInMb)
    {
        this.unsafeMemoryInMb = unsafeMemoryInMb;
        return this;
    }

    public String getEnableUnsafeInQueryExecution()
    {
        return enableUnsafeInQueryExecution;
    }

    @Config("enable.unsafe.in.query.processing")
    public CarbondataTableConfig setEnableUnsafeInQueryExecution(String enableUnsafeInQueryExecution)
    {
        this.enableUnsafeInQueryExecution = enableUnsafeInQueryExecution;
        return this;
    }

    public String getEnableUnsafeColumnPage()
    {
        return enableUnsafeColumnPage;
    }

    @Config("enable.unsafe.columnpage")
    public CarbondataTableConfig setEnableUnsafeColumnPage(String enableUnsafeColumnPage)
    {
        this.enableUnsafeColumnPage = enableUnsafeColumnPage;
        return this;
    }

    public String getEnableUnsafeSort()
    {
        return enableUnsafeSort;
    }

    @Config("enable.unsafe.sort")
    public CarbondataTableConfig setEnableUnsafeSort(String enableUnsafeSort)
    {
        this.enableUnsafeSort = enableUnsafeSort;
        return this;
    }

    public String getEnableQueryStatistics()
    {
        return enableQueryStatistics;
    }

    @Config("enable.query.statistics")
    public CarbondataTableConfig setEnableQueryStatistics(String enableQueryStatistics)
    {
        this.enableQueryStatistics = enableQueryStatistics;
        return this;
    }

    public String getBatchSize()
    {
        return batchSize;
    }

    @Config("query.vector.batchSize")
    public CarbondataTableConfig setBatchSize(String batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public String getS3A_AcesssKey()
    {
        return s3AAcesssKey;
    }

    @Config("fs.s3a.access.key")
    public CarbondataTableConfig setS3A_AcesssKey(String s3AAcesssKey)
    {
        this.s3AAcesssKey = s3AAcesssKey;
        return this;
    }

    public String getS3A_SecretKey()
    {
        return s3ASecretKey;
    }

    @Config("fs.s3a.secret.key")
    public CarbondataTableConfig setS3A_SecretKey(String s3ASecretKey)
    {
        this.s3ASecretKey = s3ASecretKey;
        return this;
    }

    public String getS3_AcesssKey()
    {
        return s3AcesssKey;
    }

    @Config("fs.s3.awsAccessKeyId")
    public CarbondataTableConfig setS3_AcesssKey(String s3AcesssKey)
    {
        this.s3AcesssKey = s3AcesssKey;
        return this;
    }

    public String getS3_SecretKey()
    {
        return s3SecretKey;
    }

    @Config("fs.s3.awsSecretAccessKey")
    public CarbondataTableConfig setS3_SecretKey(String s3SecretKey)
    {
        this.s3SecretKey = s3SecretKey;
        return this;
    }

    public String getS3N_AcesssKey()
    {
        return s3NAcesssKey;
    }

    @Config("fs.s3n.awsAccessKeyId")
    public CarbondataTableConfig setS3N_AcesssKey(String s3NAcesssKey)
    {
        this.s3NAcesssKey = s3NAcesssKey;
        return this;
    }

    public String getS3N_SecretKey()
    {
        return s3NSecretKey;
    }

    @Config("fs.s3.awsSecretAccessKey")
    public CarbondataTableConfig setS3N_SecretKey(String s3NSecretKey)
    {
        this.s3NSecretKey = s3NSecretKey;
        return this;
    }

    public String getS3EndPoint()
    {
        return endPoint;
    }

    @Config("fs.s3a.endpoint")
    public CarbondataTableConfig setS3EndPoint(String endPoint)
    {
        this.endPoint = endPoint;
        return this;
    }

    public String getPushRowFilter()
    {
        return pushRowFilter;
    }

    @Config("carbon.push.rowfilters.for.vector")
    public void setPushRowFilter(String pushRowFilter)
    {
        this.pushRowFilter = pushRowFilter;
    }
}
