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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.hetu.core.hive.dynamicfunctions.DynamicHiveScalarFunction;
import io.hetu.core.hive.dynamicfunctions.FunctionMetadata;
import io.hetu.core.hive.dynamicfunctions.RecognizedFunctions;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.hetu.core.hive.dynamicfunctions.DynamicHiveScalarFunction.EVALUATE_METHOD_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveFunctionsPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(HiveFunctionsPlugin.class);
    private static final String FUNCTION_PROPERTIES_FILE_PATH = format("etc%sudf.properties",
            File.separatorChar);
    private static final File EXTERNAL_FUNCTIONS_DIR = new File("externalFunctions");
    private static final ImmutableList<String> PLUGIN_PACKAGES = ImmutableList.<String>builder()
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("io.prestosql.hive.")
            .build();

    private String funcPropFilePath;
    private ClassLoader funcClassLoader;
    private boolean maxFunctionRunningTimeEnable;
    private long maxFuncRunningTimeInSec;
    private int functionRunningThreadPoolSize;

    public HiveFunctionsPlugin(String propFilePath, ClassLoader classLoader)
    {
        this.funcPropFilePath = requireNonNull(propFilePath, "propFilePath is null.");
        this.funcClassLoader = classLoader;
    }

    public HiveFunctionsPlugin(String propFilePath)
    {
        this.funcPropFilePath = requireNonNull(propFilePath, "propFilePath is null.");
        setFunctionClassLoader(EXTERNAL_FUNCTIONS_DIR);
    }

    private void setFunctionClassLoader(File externalFunctionsDir)
    {
        List<URL> urls = getURLs(externalFunctionsDir);
        this.funcClassLoader = new HiveFunctionsClassLoader(urls, this.getClass().getClassLoader(), PLUGIN_PACKAGES);
        log.info("Create %s, with urls: %s.", funcClassLoader, urls.toString());
    }

    private List<URL> getURLs(File dir)
    {
        List<URL> urls = new ArrayList<>();
        String dirName = dir.getName();
        if (!dir.exists() || !dir.isDirectory()) {
            log.debug("%s doesn't exist or is not a directory.", dirName);
            return urls;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            log.debug("%s is empty.", dirName);
            return urls;
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                log.error("Failed to add %s to URLs of HiveFunctionsClassLoader.", file);
            }
        }
        return urls;
    }

    public HiveFunctionsPlugin()
    {
        this(FUNCTION_PROPERTIES_FILE_PATH);
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .build();
    }

    @Override
    public Set<Object> getDynamicHiveFunctions()
    {
        Set<Object> functions = new HashSet<>();
        if (this.funcClassLoader == null) {
            return functions;
        }

        for (String funcMetadataInfo : loadFunctionMetadataFromPropertiesFile()) {
            try {
                RecognizedFunctions.addRecognizedFunction(FunctionMetadata.parseFunctionClassName(funcMetadataInfo)[1]);

                FunctionMetadata functionMetadata = new FunctionMetadata(funcMetadataInfo, this.funcClassLoader);
                Method[] methods = functionMetadata.getClazz().getMethods();
                for (Method method : methods) {
                    if (method.getName().equals(EVALUATE_METHOD_NAME)) {
                        functions.add(createDynamicHiveScalarFunction(functionMetadata, method));
                    }
                }
            }
            catch (PrestoException e) {
                log.error("Cannot load function: %s, with exception %s", funcMetadataInfo, e);
            }
        }
        return functions;
    }

    private List<String> loadFunctionMetadataFromPropertiesFile()
    {
        List<String> functionNames = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(this.funcPropFilePath)))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                log.info("Loading external function metadata: %s.", line);
                functionNames.add(line);
            }
        }
        catch (IOException e) {
            log.error("Cannot load function metadata from function properties file %s, with IOException: %s",
                    this.funcPropFilePath, e);
        }
        return functionNames;
    }

    private DynamicHiveScalarFunction createDynamicHiveScalarFunction(FunctionMetadata funcMetadata, Method method)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.funcClassLoader)) {
            return new DynamicHiveScalarFunction(funcMetadata, method.getGenericParameterTypes(), method.getGenericReturnType(),
                    this.funcClassLoader, this.maxFunctionRunningTimeEnable, this.maxFuncRunningTimeInSec, this.functionRunningThreadPoolSize);
        }
    }

    @Override
    public void setExternalFunctionsDir(File externalFunctionsDir)
    {
        if (!EXTERNAL_FUNCTIONS_DIR.equals(externalFunctionsDir)) {
            setFunctionClassLoader(externalFunctionsDir);
        }
    }

    @Override
    public void setMaxFunctionRunningTimeInSec(long time)
    {
        this.maxFuncRunningTimeInSec = time;
    }

    @Override
    public void setMaxFunctionRunningTimeEnable(boolean enable)
    {
        this.maxFunctionRunningTimeEnable = enable;
    }

    @Override
    public void setFunctionRunningThreadPoolSize(int size)
    {
        this.functionRunningThreadPoolSize = size;
    }
}
