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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spiller.Spiller;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;

public class ReuseExchangeSlotUtils
{
    private ReuseExchangeOperator.STRATEGY strategy;
    private Integer slot;
    private List<Page> pageCaches;
    private List<Page> pagesToSpill;
    private ConcurrentLinkedQueue<String> sourceIdList;
    private Optional<Spiller> spiller;
    private int pagesWritten;
    private int curConsumerRefCount;
    private int totalConsumerCount;
    private OperatorContext operatorContext;

    public ReuseExchangeSlotUtils(ReuseExchangeOperator.STRATEGY strategy, Integer slot, OperatorContext operatorContext, int curConsumerRefCount)
    {
        this.strategy = strategy;
        this.slot = slot;

        if (strategy.equals(REUSE_STRATEGY_PRODUCER)) {
            this.operatorContext = operatorContext;
            this.curConsumerRefCount = curConsumerRefCount;
            this.totalConsumerCount = curConsumerRefCount;
            pagesWritten = 0;
        }

        pageCaches = new ArrayList<>();
        pagesToSpill = new ArrayList<>();
        sourceIdList = new ConcurrentLinkedQueue<>();
        spiller = Optional.empty();
    }

    public ReuseExchangeOperator.STRATEGY getStrategy()
    {
        return strategy;
    }

    public void setStrategy(ReuseExchangeOperator.STRATEGY strategy)
    {
        this.strategy = strategy;
    }

    public Integer getSlot()
    {
        return slot;
    }

    public void setSlot(Integer slot)
    {
        this.slot = slot;
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

    public ConcurrentLinkedQueue<String> getSourceIdList()
    {
        return sourceIdList;
    }

    public void setSourceIdList(ConcurrentLinkedQueue<String> sourceIdList)
    {
        this.sourceIdList = sourceIdList;
    }

    public void addToSourceIdList(String sourceIdString)
    {
        this.sourceIdList.add(sourceIdString);
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

    public int getCurConsumerRefCount()
    {
        return curConsumerRefCount;
    }

    public void setCurConsumerRefCount(int curConsumerRefCount)
    {
        this.curConsumerRefCount = curConsumerRefCount;
    }

    public int getTotalConsumerCount()
    {
        return totalConsumerCount;
    }

    public void setTotalConsumerCount(int totalConsumerCount)
    {
        this.totalConsumerCount = totalConsumerCount;
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
