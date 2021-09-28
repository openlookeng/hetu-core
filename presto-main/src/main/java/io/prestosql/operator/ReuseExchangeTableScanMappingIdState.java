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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spiller.Spiller;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;

public class ReuseExchangeTableScanMappingIdState
{
    private ReuseExchangeOperator.STRATEGY strategy;
    private UUID reuseTableScanMappingId;
    private List<Page> pageCaches;
    private List<Page> pagesToSpill;
    private ConcurrentLinkedQueue<String> sourceNodeModifiedIdList;
    private Optional<Spiller> spiller;
    private int pagesWritten;
    private int curConsumerScanNodeRefCount;
    private int totalConsumerScanNodeCount;
    private OperatorContext operatorContext;
    public boolean cacheUpdateInProgress;

    public ReuseExchangeTableScanMappingIdState(ReuseExchangeOperator.STRATEGY strategy, UUID reuseTableScanMappingId, OperatorContext operatorContext, int curConsumerScanNodeRefCount)
    {
        this.strategy = strategy;
        this.reuseTableScanMappingId = reuseTableScanMappingId;

        if (strategy.equals(REUSE_STRATEGY_PRODUCER)) {
            this.operatorContext = operatorContext;
            this.curConsumerScanNodeRefCount = curConsumerScanNodeRefCount;
            this.totalConsumerScanNodeCount = curConsumerScanNodeRefCount;
            pagesWritten = 0;
        }

        pageCaches = new ArrayList<>();
        pagesToSpill = new ArrayList<>();
        sourceNodeModifiedIdList = new ConcurrentLinkedQueue<>();
        spiller = Optional.empty();
        cacheUpdateInProgress = false;
    }

    public ReuseExchangeOperator.STRATEGY getStrategy()
    {
        return strategy;
    }

    public void setStrategy(ReuseExchangeOperator.STRATEGY strategy)
    {
        this.strategy = strategy;
    }

    public UUID getReuseTableScanMappingId()
    {
        return reuseTableScanMappingId;
    }

    public void setReuseTableScanMappingId(UUID reuseTableScanMappingId)
    {
        this.reuseTableScanMappingId = reuseTableScanMappingId;
    }

    public List<Page> getPageCaches()
    {
        return pageCaches;
    }

    public void setPageCaches(List<Page> pageCaches)
    {
        this.pageCaches = pageCaches;
    }

    public List<Page> getPagesToSpill()
    {
        return pagesToSpill;
    }

    public void setPagesToSpill(List<Page> pagesToSpill)
    {
        this.pagesToSpill = pagesToSpill;
    }

    public ConcurrentLinkedQueue<String> getSourceNodeModifiedIdList()
    {
        return sourceNodeModifiedIdList;
    }

    public void setSourceNodeModifiedIdList(ConcurrentLinkedQueue<String> sourceNodeModifiedIdList)
    {
        this.sourceNodeModifiedIdList = sourceNodeModifiedIdList;
    }

    public void addToSourceNodeModifiedIdList(String sourceNodeModifiedId)
    {
        this.sourceNodeModifiedIdList.add(sourceNodeModifiedId);
    }

    public Optional<Spiller> getSpiller()
    {
        return spiller;
    }

    public void setSpiller(Optional<Spiller> spiller)
    {
        this.spiller = spiller;
    }

    public int getPagesWrittenCount()
    {
        return pagesWritten;
    }

    public void setPagesWritten(int pagesWritten)
    {
        this.pagesWritten = pagesWritten;
    }

    public int getCurConsumerScanNodeRefCount()
    {
        return this.curConsumerScanNodeRefCount;
    }

    public void setCurConsumerScanNodeRefCount(int curConsumerScanNodeRefCount)
    {
        this.curConsumerScanNodeRefCount = curConsumerScanNodeRefCount;
    }

    public int getTotalConsumerScanNodeCount()
    {
        return totalConsumerScanNodeCount;
    }

    public void setTotalConsumerScanNodeCount(int totalConsumerScanNodeCount)
    {
        this.totalConsumerScanNodeCount = totalConsumerScanNodeCount;
    }

    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public void setOperatorContext(OperatorContext operatorContext)
    {
        this.operatorContext = operatorContext;
    }

    public void addPage(Page page)
    {
        if (this.pageCaches != null) {
            this.pageCaches.add(page);
        }
    }

    public void clearPagesToSpill()
    {
        this.pagesToSpill.clear();
    }
}
