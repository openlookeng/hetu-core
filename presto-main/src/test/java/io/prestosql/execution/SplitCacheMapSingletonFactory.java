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

import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.service.PropertyService;

import java.lang.reflect.Field;

/**
 * Only intended for tests. Singletons are bad design and are messy
 * to test.
 *
 * This factory is intended for tests to create new SplitCacheMap
 * whenever required.
 */
public class SplitCacheMapSingletonFactory
{
    private SplitCacheMapSingletonFactory()
    {
        //utility class. do nothing here.
    }

    public static synchronized SplitCacheMap createInstance()
    {
        return createInstance(true);
    }

    public static synchronized SplitCacheMap createInstance(boolean enabled)
    {
        //way to hack around singleton object - Only intended for tests
        try {
            Field field = SplitCacheMap.class.getDeclaredField("splitCacheMap");
            field.setAccessible(true);
            field.set(null, null);
            PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, enabled);
            return SplitCacheMap.getInstance();
        }
        catch (Exception e) {
            throw new IllegalStateException("Singleton creation failed!");
        }
    }
}
