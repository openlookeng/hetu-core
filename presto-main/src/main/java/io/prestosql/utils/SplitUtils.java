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
package io.prestosql.utils;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.prestosql.heuristicindex.SplitFilter;
import io.prestosql.heuristicindex.SplitFilterFactory;
import io.prestosql.metadata.Split;
import io.prestosql.split.SplitSource;

import java.util.List;

public class SplitUtils
{
    private SplitUtils()
    {
    }

    public static List<Split> getFilteredSplit(List<Predicate> predicateList, SplitSource.SplitBatch nextSplits)
    {
        if (predicateList.isEmpty()) {
            return nextSplits.getSplits();
        }

        //hetu: apply filtering
        List<Split> filteredSplits = nextSplits.getSplits();
        //hetu: build splitFilter according to the predicate
        //hetu: careful! the same thread that injects the module should load the plugins
        Injector injector = Guice.createInjector(new FilterModule());
        SplitFilterFactory splitFilterFactory = injector.getInstance(SplitFilterFactory.class);
        for (Predicate predicate : predicateList) {
            //hetu: get filter for each predicate
            // the SplitFilterFactory will return a SplitFilter that has the applicable indexes
            // based on the predicate, but no filtering has been performed yet
            SplitFilter splitFilter = splitFilterFactory.getFilter(predicate, filteredSplits);
            //oneqeury: do filter on splits to remove invalid splits from filteredSplits
            filteredSplits = splitFilter.filter(filteredSplits, predicate.getValue());
        }
        return filteredSplits;
    }

    public static String getSplitKey(Split split)
    {
        return String.format("%s:%s", split.getCatalogName(), split.getConnectorSplit().getFilePath());
    }
}
