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
package io.hetu.core.hive.dynamicfunctions;

import io.hetu.core.hive.dynamicfunctions.utils.HetuTypeUtil;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.annotation.UsedByGeneratedCode;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.function.DynamicSqlScalarFunction;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.Reflection;
import org.apache.commons.lang3.ClassUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.hetu.core.hive.dynamicfunctions.utils.HetuTypeUtil.getTypeSignature;
import static io.hetu.core.hive.dynamicfunctions.utils.HetuTypeUtil.getTypeSignatures;
import static io.hetu.core.hive.dynamicfunctions.utils.HiveObjectTranslator.translateFromHiveObject;
import static io.hetu.core.hive.dynamicfunctions.utils.HiveObjectTranslator.translateToHiveObject;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.connector.CatalogSchemaName.DEFAULT_NAMESPACE;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static java.lang.String.format;

public class DynamicHiveScalarFunction
        extends DynamicSqlScalarFunction
{
    public static final String EVALUATE_METHOD_NAME = "evaluate";
    private static final boolean MAX_FUNCTION_RUNNING_TIME_ENABLE = false;
    private static final boolean MAX_FUNCTION_RUNNING_TIME_ENABLE_FOR_TEST = true;
    private static final int EVALUATE_METHOD_PARAM_LENGTH = 5;
    private static final int FUNCTION_RUNNING_THREAD_POOL_SIZE = 100;
    private static final int MAX_FUNCTION_RUNNING_TIME_IN_SECONDS = 600;

    private ExecutorService executor;
    private FunctionMetadata funcMetadata;
    private java.lang.reflect.Type[] evalParamJavaTypes;
    private java.lang.reflect.Type evalReturnJavaTypes;
    private Type[] evalParamHetuTypes;
    private Type evalReturnHetuType;
    private ClassLoader classLoader;
    private boolean maxFuncRunningTimeEnable;
    private long maxFuncRunningTimeInSec;

    public DynamicHiveScalarFunction(FunctionMetadata funcMetadata)
    {
        this(funcMetadata, DynamicHiveScalarFunction.class.getClassLoader(), MAX_FUNCTION_RUNNING_TIME_ENABLE,
                MAX_FUNCTION_RUNNING_TIME_IN_SECONDS, FUNCTION_RUNNING_THREAD_POOL_SIZE);
    }

    public DynamicHiveScalarFunction(FunctionMetadata funcMetadata, long maxFuncRunningTimeInSec)
    {
        this(funcMetadata, DynamicHiveScalarFunction.class.getClassLoader(), MAX_FUNCTION_RUNNING_TIME_ENABLE_FOR_TEST,
                maxFuncRunningTimeInSec, FUNCTION_RUNNING_THREAD_POOL_SIZE);
    }

    public DynamicHiveScalarFunction(FunctionMetadata funcMetadata, ClassLoader classLoader, boolean maxFuncRunningTimeEnable,
                                     long maxFuncRunningTimeInSec, int functionRunningThreadPoolSize)
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, funcMetadata.getFunctionName()),
                FunctionKind.SCALAR,
                getTypeSignature(funcMetadata.getGenericReturnType(EVALUATE_METHOD_NAME)),
                getTypeSignatures(funcMetadata.getGenericParameterTypes(EVALUATE_METHOD_NAME))));
        this.funcMetadata = funcMetadata;
        this.evalParamJavaTypes = funcMetadata.getGenericParameterTypes(EVALUATE_METHOD_NAME,
                EVALUATE_METHOD_PARAM_LENGTH);
        this.evalReturnJavaTypes = funcMetadata.getGenericReturnType(EVALUATE_METHOD_NAME);
        this.classLoader = classLoader;
        this.maxFuncRunningTimeEnable = maxFuncRunningTimeEnable;
        if (maxFuncRunningTimeEnable) {
            this.maxFuncRunningTimeInSec = maxFuncRunningTimeInSec;
            this.executor = Executors.newFixedThreadPool(functionRunningThreadPoolSize);
        }
    }

    public DynamicHiveScalarFunction(FunctionMetadata funcMetadata, java.lang.reflect.Type[] genericParameterTypes,
            java.lang.reflect.Type genericReturnType)
    {
        this(funcMetadata, genericParameterTypes, genericReturnType, DynamicHiveScalarFunction.class.getClassLoader(),
                MAX_FUNCTION_RUNNING_TIME_ENABLE, MAX_FUNCTION_RUNNING_TIME_IN_SECONDS, FUNCTION_RUNNING_THREAD_POOL_SIZE);
    }

    public DynamicHiveScalarFunction(FunctionMetadata funcMetadata, java.lang.reflect.Type[] genericParameterTypes,
            java.lang.reflect.Type genericReturnType, ClassLoader classLoader, boolean maxFuncRunningTimeEnable,
                                     long maxFuncRunningTimeInSec, int functionRunningThreadPoolSize)
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, funcMetadata.getFunctionName()),
                FunctionKind.SCALAR,
                getTypeSignature(genericReturnType),
                getTypeSignatures(genericParameterTypes)));
        this.funcMetadata = funcMetadata;
        this.evalParamJavaTypes = genericParameterTypes;
        this.evalReturnJavaTypes = genericReturnType;
        this.classLoader = classLoader;
        this.maxFuncRunningTimeEnable = maxFuncRunningTimeEnable;
        if (maxFuncRunningTimeEnable) {
            this.maxFuncRunningTimeInSec = maxFuncRunningTimeInSec;
            this.executor = Executors.newFixedThreadPool(functionRunningThreadPoolSize);
        }
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize()
    {
        this.evalParamHetuTypes = getTypeSignatures(evalParamJavaTypes).stream().map(HetuTypeUtil::getType)
                .toArray(Type[]::new);
        this.evalReturnHetuType = HetuTypeUtil.getType(getTypeSignature(evalReturnJavaTypes));
        return new BuiltInScalarFunctionImplementation(true, getNullableArgumentProperties(), getMethodHandle());
    }

    private List<BuiltInScalarFunctionImplementation.ArgumentProperty> getNullableArgumentProperties()
    {
        List<BuiltInScalarFunctionImplementation.ArgumentProperty> nullableArgProps = new ArrayList<>(evalParamHetuTypes.length);
        for (Type type : evalParamHetuTypes) {
            nullableArgProps.add(type.getJavaType().isPrimitive()
                    ? valueTypeArgumentProperty(USE_BOXED_TYPE)
                    : valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
        }
        return nullableArgProps;
    }

    private MethodHandle getMethodHandle()
    {
        MethodHandle genericMethodHandle = Reflection.methodHandle(DynamicHiveScalarFunction.class,
                "invokeHive", getMethodHandleArgumentTypes(evalParamHetuTypes, true)).bindTo(this);
        Class<?> specificMethodHandleReturnType = ClassUtils.primitiveToWrapper(evalReturnHetuType.getJavaType());
        MethodType specificMethodType = MethodType.methodType(specificMethodHandleReturnType,
                getMethodHandleArgumentTypes(evalParamHetuTypes, false));
        return MethodHandles.explicitCastArguments(genericMethodHandle, specificMethodType);
    }

    private Class<?>[] getMethodHandleArgumentTypes(Type[] argTypes, boolean isGeneric)
    {
        Class<?>[] methodHandleArgumentTypes = new Class<?>[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            methodHandleArgumentTypes[i] = isGeneric
                    ? Object.class
                    : ClassUtils.primitiveToWrapper(argTypes[i].getJavaType());
        }
        return methodHandleArgumentTypes;
    }

    @UsedByGeneratedCode
    public Object invokeHive()
    {
        return evaluate();
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj)
    {
        return evaluate(obj);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1)
    {
        return evaluate(obj, obj1);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1, Object obj2)
    {
        return evaluate(obj, obj1, obj2);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1, Object obj2, Object obj3)
    {
        return evaluate(obj, obj1, obj2, obj3);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1, Object obj2, Object obj3, Object obj4)
    {
        return evaluate(obj, obj1, obj2, obj3, obj4);
    }

    private Object evaluate(Object... objs)
    {
        if (objs.length > EVALUATE_METHOD_PARAM_LENGTH) {
            throw new PrestoException(NOT_SUPPORTED,
                    format("Cannot invoke %s for function %s as %s parameters are not supported.",
                            EVALUATE_METHOD_NAME, funcMetadata.getClassName(), objs.length));
        }
        Object[] hiveObjs = new Object[objs.length];
        for (int i = 0; i < objs.length; i++) {
            hiveObjs[i] = translateToHiveObject(this.evalParamHetuTypes[i], objs[i], this.evalParamJavaTypes[i]);
        }
        Method method = getEvaluateMethod(objs.length);
        Object result;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            result = invokeFunction(method, hiveObjs);
        }
        return translateFromHiveObject(this.evalReturnHetuType, result);
    }

    private Method getEvaluateMethod(int paramNum)
    {
        Class<?>[] paramTypes = new Class[paramNum];
        for (int i = 0; i < paramNum; i++) {
            java.lang.reflect.Type paramType = this.evalParamJavaTypes[i];
            paramTypes[i] = paramType instanceof ParameterizedType
                    ? (Class<?>) ((ParameterizedType) paramType).getRawType()
                    : (Class<?>) paramType;
        }
        try {
            return this.funcMetadata.getClazz().getMethod(EVALUATE_METHOD_NAME, paramTypes);
        }
        catch (NoSuchMethodException e) {
            throw new PrestoException(NOT_FOUND, format("Cannot find %s for function %s with signature: %s.",
                    EVALUATE_METHOD_NAME, funcMetadata.getClassName(), this.getSignature()));
        }
    }

    private Object invokeFunction(Method method, Object[] hiveObjs)
    {
        if (!maxFuncRunningTimeEnable) {
            return getResult(method, hiveObjs);
        }
        else {
            Future<Object> future = executor.submit(() ->
                    getResult(method, hiveObjs));
            try {
                return future.get(this.maxFuncRunningTimeInSec, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Failed to get results for method %s of" +
                        "  function %s, with exception: %s.", EVALUATE_METHOD_NAME, funcMetadata.getInstance(), e));
            }
        }
    }

    private Object getResult(Method method, Object[] hiveObjs)
    {
        try {
            return method.invoke(this.funcMetadata.getInstance(), hiveObjs);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Cannot invoke %s for function %s," +
                    " with exception: %s.", EVALUATE_METHOD_NAME, funcMetadata.getInstance(), e));
        }
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public boolean equals(Object that)
    {
        return this.getSignature().equals(((DynamicHiveScalarFunction) that).getSignature());
    }

    @Override
    public int hashCode()
    {
        return this.getSignature().hashCode();
    }
}
