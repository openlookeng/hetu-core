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
package io.prestosql.orc;

public class OrcCacheStatsListerWrapper
{
    private final BloomFilterCacheStatsLister bloomFilterCacheStatsLister;
    private final FileTailCacheStatsLister fileTailCacheStatsLister;
    private final RowDataCacheStatsLister rowDataCacheStatsLister;
    private final RowIndexCacheStatsLister rowIndexCacheStatsLister;
    private final StripeFooterCacheStatsLister stripeFooterCacheStatsLister;

    OrcCacheStatsListerWrapper(OrcCacheStore orcCacheStore)
    {
        bloomFilterCacheStatsLister = new BloomFilterCacheStatsLister(orcCacheStore);
        fileTailCacheStatsLister = new FileTailCacheStatsLister(orcCacheStore);
        rowDataCacheStatsLister = new RowDataCacheStatsLister(orcCacheStore);
        rowIndexCacheStatsLister = new RowIndexCacheStatsLister(orcCacheStore);
        stripeFooterCacheStatsLister = new StripeFooterCacheStatsLister(orcCacheStore);
    }

    public BloomFilterCacheStatsLister getBloomFilterCacheStatsLister()
    {
        return bloomFilterCacheStatsLister;
    }

    public FileTailCacheStatsLister getFileTailCacheStatsLister()
    {
        return fileTailCacheStatsLister;
    }

    public RowDataCacheStatsLister getRowDataCacheStatsLister()
    {
        return rowDataCacheStatsLister;
    }

    public RowIndexCacheStatsLister getRowIndexCacheStatsLister()
    {
        return rowIndexCacheStatsLister;
    }

    public StripeFooterCacheStatsLister getStripeFooterCacheStatsLister()
    {
        return stripeFooterCacheStatsLister;
    }
}
