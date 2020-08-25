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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.TupleDomain;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * TableCacheInfo stores information user defined ORC Cache predicates
 * and also keeps track of splits and the workers on which those splits
 * were scheduled.
 */
public class TableCacheInfo
{
    private final String fqTableName;
    private final Map<SplitKey, String> splitWorkersMap;
    private final Set<CachePredicate> predicates;
    private LocalDateTime lastUpdated;

    public TableCacheInfo(String fqTableName)
    {
        this(fqTableName, new ConcurrentHashMap<>(500), new HashSet<>(100), LocalDateTime.now());
    }

    @JsonCreator
    public TableCacheInfo(@JsonProperty("fqTableName") String fqTableName,
                          @JsonProperty("splitWorkersMap") Map<SplitKey, String> splitWorkersMap,
                          @JsonProperty("predicates") Set<CachePredicate> predicates,
                          @JsonProperty("lastUpdated") LocalDateTime lastUpdated)
    {
        this.fqTableName = requireNonNull(fqTableName, "fqTableName is null.");
        this.splitWorkersMap = requireNonNull(splitWorkersMap, "splitWorkersMap is null.");
        this.predicates = requireNonNull(predicates, "predicates is null.");
        this.lastUpdated = requireNonNull(lastUpdated, "lastUpdated is null.");
    }

    @JsonProperty
    public String getFqTableName()
    {
        return fqTableName;
    }

    @JsonProperty
    public Map<SplitKey, String> getSplitWorkersMap()
    {
        return ImmutableMap.copyOf(splitWorkersMap);
    }

    @JsonIgnore
    public void setCachedNode(SplitKey splitKey, String nodeId)
    {
        splitWorkersMap.put(splitKey, nodeId);
        this.lastUpdated = LocalDateTime.now();
    }

    @JsonIgnore
    public String getCachedNode(SplitKey splitKey)
    {
        return splitWorkersMap.get(splitKey);
    }

    @JsonProperty
    public Set<CachePredicate> getPredicates()
    {
        return ImmutableSet.copyOf(predicates);
    }

    @JsonIgnore
    public void addCachedPredicate(CachePredicate cachePredicate)
    {
        this.predicates.add(cachePredicate);
        this.lastUpdated = LocalDateTime.now();
    }

    @JsonIgnore
    public void removeCachedPredicate(CachePredicate cachePredicate)
    {
        this.predicates.remove(cachePredicate);
        this.lastUpdated = LocalDateTime.now();
    }

    @JsonIgnore
    public Set<TupleDomain<ColumnMetadata>> getCachePredicateTupleDomains()
    {
        return ImmutableSet.copyOf(this.predicates
                .stream()
                .map(CachePredicate::getColumnMetadataTupleDomain)
                .collect(Collectors.toSet()));
    }

    @JsonProperty
    public LocalDateTime getLastUpdated()
    {
        return LocalDateTime.from(lastUpdated);
    }

    @JsonIgnore
    public String showPredicates()
    {
        StringBuilder predicatesToShow = new StringBuilder();
        this.predicates.stream().map(CachePredicate::getCachePredicateString).forEach(cachePredicateString -> predicatesToShow.append(cachePredicateString).append(System.lineSeparator()));
        predicatesToShow.append(System.lineSeparator());
        return predicatesToShow.toString();
    }

    @JsonIgnore
    public String showNodes()
    {
        StringBuilder nodesToShow = new StringBuilder();
        Set<String> cachedNodes = ImmutableSet.copyOf(this.splitWorkersMap.values());
        if (cachedNodes.isEmpty()) {
            nodesToShow.append("Nothing cached yet").append(System.lineSeparator());
        }
        else {
            cachedNodes.forEach(nodeId -> {
                nodesToShow.append(nodeId).append(System.lineSeparator());
            });
        }
        return nodesToShow.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableCacheInfo cacheInfo = (TableCacheInfo) o;
        return Objects.equals(fqTableName, cacheInfo.fqTableName) &&
                Objects.equals(splitWorkersMap, cacheInfo.splitWorkersMap) &&
                Objects.equals(predicates, cacheInfo.predicates) &&
                Objects.equals(lastUpdated, cacheInfo.lastUpdated);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fqTableName, splitWorkersMap, predicates, lastUpdated);
    }

    @Override
    public String toString()
    {
        return "TableCacheInfo{" +
                "fqTableName=" + fqTableName +
                ", splitWorkersMap=" + splitWorkersMap +
                ", predicates=" + predicates +
                ", lastUpdated=" + lastUpdated +
                '}';
    }
}
