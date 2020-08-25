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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.sql.tree.QualifiedName;

import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ThreadSafe
public class SplitCacheMap
{
    private static final Logger log = Logger.get(SplitCacheMap.class);

    private static SplitCacheMap splitCacheMap;

    //split cache metadata information, key -> FQ tablename
    private final Map<String, TableCacheInfo> tableCacheInfoMap;
    private final List<String> droppedCaches;

    public static SplitCacheMap getInstance()
    {
        if (splitCacheMap == null && PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
            synchronized (SplitCacheMap.class) {
                if (splitCacheMap == null) {
                    splitCacheMap = new SplitCacheMap();
                }
            }
        }
        return splitCacheMap;
    }

    private SplitCacheMap()
    {
        this.tableCacheInfoMap = new ConcurrentHashMap<>(100);
        this.droppedCaches = new CopyOnWriteArrayList<>();
    }

    public void addCache(QualifiedName tableName, TupleDomain<ColumnMetadata> predicate, String predicateString)
    {
        String fqTableName = tableName.toString();
        CachePredicate cachePredicate = new CachePredicate(predicate, predicateString);
        TableCacheInfo tableCacheInfo = this.tableCacheInfoMap.computeIfAbsent(fqTableName, ignored -> new TableCacheInfo(fqTableName));
        tableCacheInfo.addCachedPredicate(cachePredicate);
        log.info("Adding new cached predicate %s on table %s.", cachePredicate, tableName);
    }

    public boolean cacheExists(QualifiedName tableName)
    {
        return tableCacheInfoMap.containsKey(tableName.toString());
    }

    public void dropCache(QualifiedName tableName, Optional<String> predicateString)
    {
        if (!predicateString.isPresent()) {
            this.droppedCaches.add(tableName.toString());
            this.tableCacheInfoMap.remove(tableName.toString());
            return;
        }

        boolean dropFlag = false;
        TableCacheInfo tableCacheInfo = tableCacheInfoMap.get(tableName.toString());
        Set<CachePredicate> cachePredicates = tableCacheInfo.getPredicates();
        for (CachePredicate cachePredicate : cachePredicates) {
            if (cachePredicate.getCachePredicateString().equalsIgnoreCase(predicateString.get())) {
                tableCacheInfo.removeCachedPredicate(cachePredicate);
                dropFlag = true;
                break;
            }
        }

        if (!dropFlag) {
            throw new RuntimeException(String.format("Cache predicate '%s' does not exist", predicateString.get()));
        }

        if (tableCacheInfo.getPredicates().isEmpty()) {
            this.droppedCaches.add(tableName.toString());
            this.tableCacheInfoMap.remove(tableName.toString());
        }
    }

    public void dropCache()
    {
        this.droppedCaches.addAll(this.tableCacheInfoMap.keySet());
        this.tableCacheInfoMap.clear();
    }

    public Optional<String> getCachedNodeId(SplitKey splitKey)
    {
        TableCacheInfo tableCache = tableCacheInfoMap.get(splitKey.getQualifiedTableName().toString());
        if (tableCache == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(tableCache.getCachedNode(splitKey));
    }

    public void addCachedNode(SplitKey splitKey, String workerId)
    {
        TableCacheInfo tableCache = tableCacheInfoMap.get(splitKey.getQualifiedTableName().toString());
        if (tableCache == null) {
            return;
        }
        tableCache.setCachedNode(splitKey, workerId);
    }

    public Map<String, TableCacheInfo> showCache()
    {
        Map<String, TableCacheInfo> cacheInfoByTableName = new LinkedHashMap<>();
        tableCacheInfoMap.keySet().forEach(fqTableName -> cacheInfoByTableName.putAll(showCache(fqTableName)));
        return cacheInfoByTableName;
    }

    public Map<String, TableCacheInfo> showCache(String fqTableName)
    {
        Map<String, TableCacheInfo> cacheInfoByTableName = new LinkedHashMap<>();
        TableCacheInfo tableCache = tableCacheInfoMap.get(fqTableName);
        if (tableCache != null) {
            cacheInfoByTableName.put(fqTableName, tableCache);
        }
        return cacheInfoByTableName;
    }

    public Set<TupleDomain<ColumnMetadata>> getCachePredicateTupleDomains(QualifiedName tableName)
    {
        return getCachePredicateTupleDomains(tableName.toString());
    }

    public Set<TupleDomain<ColumnMetadata>> getCachePredicateTupleDomains(String fqTableName)
    {
        TableCacheInfo tableCacheInfo = tableCacheInfoMap.get(fqTableName);
        if (tableCacheInfo == null) {
            return ImmutableSet.of();
        }
        return tableCacheInfo.getCachePredicateTupleDomains();
    }

    //the following methods are added for the purpose of synchronizing with
    //state store. SplitCacheMap instance construction  need to be refactored
    //and instantiated using Guice
    public Map<String, TableCacheInfo> tableCacheInfoMap()
    {
        return ImmutableMap.copyOf(this.tableCacheInfoMap);
    }

    public List<String> getAndClearDroppedCaches()
    {
        List<String> droppedCaches = ImmutableList.copyOf(this.droppedCaches);
        this.droppedCaches.clear();
        return droppedCaches;
    }

    public void setTableCacheInfo(String fqTableName, TableCacheInfo tableCacheInfo)
    {
        this.tableCacheInfoMap.put(fqTableName, tableCacheInfo);
    }

    public void removeTableCacheInfo(String fqTableName)
    {
        this.tableCacheInfoMap.remove(fqTableName);
    }
}
