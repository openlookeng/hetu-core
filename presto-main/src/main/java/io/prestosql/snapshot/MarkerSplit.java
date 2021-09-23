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
package io.prestosql.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.snapshot.MarkerPage;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A marker split is a signal for a task to take a snapshot of its internal state, or to restore to a previous snapshot of its internal state.
 */
public class MarkerSplit
        implements ConnectorSplit
{
    private final CatalogName catalogName;
    private final long snapshotId;
    private final boolean isResuming;

    public static MarkerSplit snapshotSplit(CatalogName catalogName, long snapshotId)
    {
        return new MarkerSplit(catalogName, snapshotId, false);
    }

    public static MarkerSplit resumeSplit(CatalogName catalogName, long snapshotId)
    {
        return new MarkerSplit(catalogName, snapshotId, true);
    }

    @JsonCreator
    public MarkerSplit(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("isResuming") boolean isResuming)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.snapshotId = snapshotId;
        this.isResuming = isResuming;
    }

    @Override
    public boolean isSplitEmpty()
    {
        return true;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty("isResuming")
    public boolean isResuming()
    {
        return isResuming;
    }

    public MarkerPage toMarkerPage()
    {
        return new MarkerPage(snapshotId, isResuming);
    }
}
