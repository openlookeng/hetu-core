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
package io.hetu.core.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveFunctionsClassLoader
        extends URLClassLoader
{
    private static final ClassLoader PLATFORM_CLASS_LOADER = findPlatformClassLoader();

    private final ClassLoader pluginClassLoader;
    private final List<String> pluginPackages;
    private final List<String> pluginResources;

    public HiveFunctionsClassLoader(
            List<URL> urls,
            ClassLoader pluginClassLoader,
            Iterable<String> pluginPackages)
    {
        this(urls,
                pluginClassLoader,
                pluginPackages,
                Iterables.transform(pluginPackages, HiveFunctionsClassLoader::classNameToResource));
    }

    private HiveFunctionsClassLoader(
            List<URL> urls,
            ClassLoader pluginClassLoader,
            Iterable<String> pluginPackages,
            Iterable<String> pluginResources)
    {
        super(urls.toArray(new URL[urls.size()]), PLATFORM_CLASS_LOADER);
        this.pluginClassLoader = requireNonNull(pluginClassLoader, "pluginClassLoader is null");
        this.pluginPackages = ImmutableList.copyOf(pluginPackages);
        this.pluginResources = ImmutableList.copyOf(pluginResources);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name)) {
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                return resolveClass(cachedClass, resolve);
            }
            if (isPluginClass(name)) {
                return resolveClass(pluginClassLoader.loadClass(name), resolve);
            }
            return super.loadClass(name, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name)
    {
        if (isPluginResource(name)) {
            return pluginClassLoader.getResource(name);
        }
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        if (isPluginClass(name)) {
            return pluginClassLoader.getResources(name);
        }
        return super.getResources(name);
    }

    private boolean isPluginClass(String name)
    {
        return pluginPackages.stream().anyMatch(name::startsWith);
    }

    private boolean isPluginResource(String name)
    {
        return pluginResources.stream().anyMatch(name::startsWith);
    }

    private static String classNameToResource(String className)
    {
        return className.replace('.', '/');
    }

    @SuppressWarnings("JavaReflectionMemberAccess")
    private static ClassLoader findPlatformClassLoader()
    {
        try {
            Method method = ClassLoader.class.getMethod("getPlatformClassLoader");
            return (ClassLoader) method.invoke(null);
        }
        catch (NoSuchMethodException ignored) {
            return null;
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }
}
