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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class RunningTaskHashMap
{
    public HashMap<String, List<String>> hashMap;

    public RunningTaskHashMap()
    {
        this.hashMap = new HashMap<>();
    }

    public List<String> put(String key, List<String> value)
    {
        synchronized (this) {
            hashMap.put(key, value);
            return value;
        }
    }

    public List<String> get(String key)
    {
        synchronized (this) {
            if (hashMap.get(key) == null) {
                List<String> list = new CopyOnWriteArrayList<>();
                hashMap.put(key, list);
                return hashMap.get(key);
            }
            else {
                return hashMap.get(key);
            }
        }
    }

    public List<String> removeList(Object key)
    {
        synchronized (this) {
            return hashMap.remove(key);
        }
    }

    public boolean removeThread(Object key, String threadName)
    {
        synchronized (this) {
            if (hashMap.get(key) != null) {
                return hashMap.get(key).remove(threadName);
            }
            else {
                return true;
            }
        }
    }
}
