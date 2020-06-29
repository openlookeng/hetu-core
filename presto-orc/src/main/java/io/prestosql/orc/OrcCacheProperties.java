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
package io.prestosql.orc;

public final class OrcCacheProperties
{
    private boolean fileTailCacheEnabled;
    private boolean stripeFooterCacheEnabled;
    private boolean rowIndexCacheEnabled;
    private boolean bloomFilterCacheEnabled;
    private boolean rowDataCacheEnabled;

    public OrcCacheProperties()
    {
        //default constructor
    }

    public OrcCacheProperties(boolean fileTailCacheEnabled, boolean stripeFooterCacheEnabled,
                              boolean rowIndexCacheEnabled, boolean bloomFilterCacheEnabled,
                              boolean rowDataCacheEnabled)
    {
        this.fileTailCacheEnabled = fileTailCacheEnabled;
        this.stripeFooterCacheEnabled = stripeFooterCacheEnabled;
        this.rowIndexCacheEnabled = rowIndexCacheEnabled;
        this.bloomFilterCacheEnabled = bloomFilterCacheEnabled;
        this.rowDataCacheEnabled = rowDataCacheEnabled;
    }

    public boolean isFileTailCacheEnabled()
    {
        return fileTailCacheEnabled;
    }

    public boolean isStripeFooterCacheEnabled()
    {
        return stripeFooterCacheEnabled;
    }

    public boolean isRowIndexCacheEnabled()
    {
        return rowIndexCacheEnabled;
    }

    public boolean isBloomFilterCacheEnabled()
    {
        return bloomFilterCacheEnabled;
    }

    public boolean isRowDataCacheEnabled()
    {
        return rowDataCacheEnabled;
    }

    @Override
    public String toString()
    {
        return "OrcCacheProperties{" +
                "fileTailCacheEnabled=" + fileTailCacheEnabled +
                ", stripeFooterCacheEnabled=" + stripeFooterCacheEnabled +
                ", rowIndexCacheEnabled=" + rowIndexCacheEnabled +
                ", bloomFilterCacheEnabled=" + bloomFilterCacheEnabled +
                ", rowDataCacheEnabled=" + rowDataCacheEnabled +
                '}';
    }
}
