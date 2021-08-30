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

package io.prestosql.spi;

public class PartialAndFinalAggregationType
{
    private boolean isSortAggregation;
    private boolean isPartialAsSortAndFinalAsHashAggregation;

    public PartialAndFinalAggregationType()
    {
        isSortAggregation = false;
        isPartialAsSortAndFinalAsHashAggregation = false;
    }

    public boolean isSortAggregation()
    {
        return isSortAggregation;
    }

    public void setSortAggregation(boolean sortAggregation)
    {
        isSortAggregation = sortAggregation;
    }

    public boolean isPartialAsSortAndFinalAsHashAggregation()
    {
        return isPartialAsSortAndFinalAsHashAggregation;
    }

    public void setPartialAsSortAndFinalAsHashAggregation(boolean partialAsSortAndFinalAsHashAggregation)
    {
        isPartialAsSortAndFinalAsHashAggregation = partialAsSortAndFinalAsHashAggregation;
    }
}
