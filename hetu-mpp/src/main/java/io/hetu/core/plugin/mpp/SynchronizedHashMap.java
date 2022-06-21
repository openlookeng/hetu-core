/*
 * Copyright (C) 2022-2022. Yijian Cheng. All rights reserved.
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
package io.hetu.core.plugin.mpp;

import io.hetu.core.plugin.mpp.scheduler.utils.Const;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SynchronizedHashMap<K, V>
{
    public HashMap<K, V> hashMap;
    public String mapName;
    public static final String TABLE_STATUS_MAP = "tableStatus";
    public static final String ETL_INFO_MAP = "etlInfo";

    public static final String TABLE_SCHEMA_MAP = "schemas";

    public HashMap<String, HashMap<K, V>> threadHashMap;

    public List<Object> tableStatusList;

    public ExpiringMap<K, V> expiringMap;

    public HashMap<String, Integer> gaussDbTaskStatusMap;

    public int second;

    private SynchronizedHashMap()
    {
    }

    public SynchronizedHashMap(String mapName)
    {
        this.mapName = mapName;
        switch (mapName){
            case TABLE_STATUS_MAP:
                threadHashMap = new HashMap();
                tableStatusList = new ArrayList<>();
                gaussDbTaskStatusMap = new HashMap<>();
                break;
            case ETL_INFO_MAP:
                hashMap = new HashMap();
                break;
            default:
                break;
        }
    }

    public SynchronizedHashMap(int second, int maxSize)
    {
        this.second = second;
        this.mapName = TABLE_SCHEMA_MAP;
        expiringMap = ExpiringMap.builder()
                .maxSize(maxSize)
                .expirationPolicy(ExpirationPolicy.ACCESSED)
                .expiration(second, TimeUnit.SECONDS)
                .build();
    }

    public V put(K key, V value, String threadName)
    {
        switch (mapName){
            case TABLE_STATUS_MAP:
                synchronized (this) {
                    if (threadHashMap.get(threadName) == null) {
                        HashMap<K, V> kvHashMap = new HashMap<>();
                        V result = kvHashMap.put(key, value);
                        threadHashMap.put(threadName, kvHashMap);
                        if (value instanceof Integer) {
                            if (((Integer) value).intValue() == 0) {
                                tableStatusList.add(key);
                            }
                        }
                        return result;
                    }
                    else {
                        HashMap<K, V> kvHashMap = threadHashMap.get(threadName);
                        V result = kvHashMap.put(key, value);
                        threadHashMap.put(threadName, kvHashMap);
                        if (value instanceof Integer) {
                            if (((Integer) value).intValue() == 0) {
                                tableStatusList.add(key);
                            }
                        }
                        return result;
                    }
                }
            case TABLE_SCHEMA_MAP:
                synchronized (this) {
                    return expiringMap.put(key, value);
                }
            default:
                synchronized (this) {
                    return hashMap.put(key, value);
                }
        }
    }

    public V put(K key, V value)
    {
        switch (mapName) {
            case TABLE_STATUS_MAP:
                synchronized (this) {
                    String threadName = getThreadName();
                    if (threadHashMap.get(threadName) == null) {
                        HashMap<K, V> kvHashMap = new HashMap<>();
                        V result = kvHashMap.put(key, value);
                        threadHashMap.put(threadName, kvHashMap);
                        if (value instanceof Integer) {
                            if (((Integer) value).intValue() == 0) {
                                tableStatusList.add(key);
                            }
                        }
                        return result;
                    }
                    else {
                        HashMap<K, V> kvHashMap = threadHashMap.get(threadName);
                        V result = kvHashMap.put(key, value);
                        threadHashMap.put(threadName, kvHashMap);
                        if (value instanceof Integer) {
                            if (((Integer) value).intValue() == 0) {
                                tableStatusList.add(key);
                            }
                        }
                        return result;
                    }
                }
            case TABLE_SCHEMA_MAP:
                synchronized (this) {
                    return expiringMap.put(key, value);
                }
            default:
                synchronized (this) {
                    return hashMap.put(key, value);
                }
        }
    }

    public String getThreadName()
    {
        String threadName = Thread.currentThread().getName();
        threadName = threadName.substring(0, threadName.lastIndexOf(Const.idSeparator));
        return threadName;
    }

    /**
     * get put remove
     * @param key
     * @return
     */
    public boolean containsKey(Object key)
    {
        switch (mapName){
            case TABLE_STATUS_MAP:
                synchronized (this) {
                    String threadName = getThreadName();
                    if (!threadHashMap.containsKey(threadName)) {
                        return false;
                    }
                    else {
                        HashMap<K, V> kvHashMap = threadHashMap.get(threadName);
                        boolean result = kvHashMap.containsKey(key);
                        return result;
                    }
                }
            case TABLE_SCHEMA_MAP:
                synchronized (this) {
                    return expiringMap.containsKey(key);
                }
            default:
                synchronized (this) {
                    return hashMap.containsKey(key);
                }
        }
    }

    public V get(Object key)
    {
        switch (mapName){
            case TABLE_STATUS_MAP:
                synchronized (this) {
                    String threadName = getThreadName();
                    if (!threadHashMap.containsKey(threadName)) {
                        return null;
                    }
                    else {
                        HashMap<K, V> kvHashMap = threadHashMap.get(threadName);
                        V value = kvHashMap.get(key);
                        return value;
                    }
                }
            case TABLE_SCHEMA_MAP:
                synchronized (this) {
                    return expiringMap.get(key);
                }
            default:
                synchronized (this) {
                    return hashMap.get(key);
                }
        }
    }

    /**
     * 仅限于tablestatus开头判断
     * @param key
     * @return
     */
    public boolean tableStatusKeysExists(Object key)
    {
        return tableStatusList.contains(key);
    }

    public V remove(Object key)
    {
        switch (mapName){
            case TABLE_STATUS_MAP:
                synchronized (this) {
                    String threadName = getThreadName();
                    if (!threadHashMap.containsKey(threadName)) {
                        return null;
                    }
                    else {
                        HashMap<K, V> kvHashMap = threadHashMap.get(threadName);
                        V value = kvHashMap.remove(key);
                        threadHashMap.remove(threadName);
                        //可能存在问题
                        tableStatusList.remove(key);
                        return value;
                    }
                }
            case TABLE_SCHEMA_MAP:
                synchronized (this) {
                    return expiringMap.remove(key);
                }
            default:
                synchronized (this) {
                    return hashMap.remove(key);
                }
        }
    }
}
