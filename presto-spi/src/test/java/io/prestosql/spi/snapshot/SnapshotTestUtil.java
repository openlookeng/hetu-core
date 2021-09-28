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
package io.prestosql.spi.snapshot;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class SnapshotTestUtil
{
    private SnapshotTestUtil() {}

    public static Map<String, Object> toSimpleSnapshotMapping(Object snapshot)
    {
        Map<String, Object> result = new HashMap<>();
        for (Field field : snapshot.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object fieldValue = null;
            try {
                fieldValue = field.get(snapshot);
            }
            catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if (fieldValue == null) {
                continue;
            }
            if (fieldValue instanceof Number || fieldValue.getClass().equals(Boolean.class) || fieldValue instanceof String) {
                result.put(field.getName(), fieldValue);
            }
            else if (fieldValue.getClass().isArray() && fieldValue.getClass().getComponentType().isPrimitive()) {
                if (Array.getLength(fieldValue) > 100) {
                    result.put(field.getName(), fieldValue.getClass());
                }
                else {
                    List<Object> resultList = new ArrayList<>();
                    for (int i = 0; i < Array.getLength(fieldValue); i++) {
                        resultList.add(Array.get(fieldValue, i));
                    }
                    result.put(field.getName(), resultList);
                }
            }
        }
        return result;
    }

    public static Object toFullSnapshotMapping(Object obj)
    {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Number || obj.getClass().equals(Boolean.class) || obj instanceof String) {
            return obj;
        }
        else if (obj instanceof Map) {
            return ((Map) obj).size();
        }
        else if (obj instanceof List) {
            if (((List) obj).size() > 100) {
                return obj.getClass();
            }
            List<?> stateList = (List) obj;
            List<Object> resultList = new ArrayList<>();
            for (Object o : stateList) {
                resultList.add(toFullSnapshotMapping(o));
            }
            return resultList;
        }
        else if (obj instanceof Set) {
            Set<?> stateSet = (Set) obj;
            Set<Object> resultSet = new HashSet<>();
            for (Object o : stateSet) {
                resultSet.add(toFullSnapshotMapping(o));
            }
            return resultSet;
        }
        else if (obj.getClass().isArray()) {
            if (Array.getLength(obj) > 30) {
                return obj.getClass();
            }
            List<Object> resultList = new ArrayList<>();
            for (int i = 0; i < Array.getLength(obj); i++) {
                resultList.add(toFullSnapshotMapping(Array.get(obj, i)));
            }
            return resultList;
        }

        Map<String, Object> result = new HashMap<>();
        for (Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            try {
                // Too complex to compare groupBy content
                if (!field.getName().equals("groupBy")) {
                    result.put(field.getName(), toFullSnapshotMapping(field.get(obj)));
                }
            }
            catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
