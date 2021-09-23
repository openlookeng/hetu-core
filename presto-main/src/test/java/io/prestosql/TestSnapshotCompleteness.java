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
package io.prestosql;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.reflect.ClassPath;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.fail;

public class TestSnapshotCompleteness
{
    private final Map<Class<?>, Integer> capturedFieldCount = new HashMap<>();
    private final Multimap<Class<?>, String> failures = HashMultimap.create();
    private final Map<Class<?>, Set<String>> unusedUncapturedFields = new HashMap<>();

    @Test
    public void testSnapshotCompleteness()
            throws Exception
    {
        List<Class<?>> allRestorableClasses = ClassPath.from(ClassLoader.getSystemClassLoader())
                .getAllClasses()
                .stream()
                .filter(ci -> ci.getPackageName().startsWith("io.prestosql") && !ci.getPackageName().startsWith("io.prestosql.hadoop"))
                .map(ClassPath.ClassInfo::load)
                .filter(clazz -> !clazz.isInterface())
                .filter(Restorable.class::isAssignableFrom)
                .collect(Collectors.toList());

        for (Class<?> clazz : allRestorableClasses) {
            test(clazz);
        }

        if (!failures.isEmpty()) {
            String content = failures.entries().stream().map(Map.Entry::toString).collect(Collectors.joining("\n"));
            fail(content);
        }

        if (!unusedUncapturedFields.isEmpty()) {
            String content = unusedUncapturedFields.entrySet().stream().map(Map.Entry::toString).collect(Collectors.joining("\n"));
            System.out.println("There are unused uncaptured fields:\n" + content);
        }
    }

    private boolean containsCaptureMethod(Class<?> clazz)
    {
        // Don't worry about "restore", because sometimes restore is not done on an existing object, but to create a new one
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals("capture")) {
                return true;
            }
        }
        return false;
    }

    private boolean isImmutable(Type type)
    {
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;
            if (clazz.isPrimitive() || clazz.isEnum()) {
                return true;
            }
        }

        if (type == String.class
                || type == Boolean.class
                || type == Byte.class
                || type == Character.class
                || type == Short.class
                || type == Integer.class
                || type == Long.class
                || type == Float.class
                || type == Double.class
                || type == BigInteger.class
                || type == BigDecimal.class
                || type == OptionalInt.class
                || type == OptionalLong.class
                || type == OptionalDouble.class
                || type == RestorableConfig.class) {
            return true;
        }

        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return parameterizedType.getOwnerType() == Optional.class
                    && isImmutable(parameterizedType.getActualTypeArguments()[0]);
        }

        return false;
    }

    private RestorableConfig getConfigAnnotation(Class<?> clazz)
            throws Exception
    {
        RestorableConfig config = clazz.getAnnotation(RestorableConfig.class);
        if (config == null) {
            Optional<Field> restorableConfigField = Arrays.stream(clazz.getDeclaredFields())
                    .filter(field -> field.getName().equals("restorableConfig") && field.getGenericType() == RestorableConfig.class)
                    .findAny();
            if (restorableConfigField.isPresent()) {
                config = restorableConfigField.get().getAnnotation(RestorableConfig.class);
            }
        }
        return config;
    }

    private void getAllFields(List<String> allFields, Class<?> clazz)
            throws Exception
    {
        //Find fields in non-Restorable base class.
        if (clazz.getSuperclass() != null && !Restorable.class.isAssignableFrom(clazz.getSuperclass())) {
            getAllFields(allFields, clazz.getSuperclass());
        }

        //My own fields
        List<String> currentClassFields = Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()) && !(Modifier.isFinal(field.getModifiers()) && isImmutable(field.getGenericType())))
                .map(field -> clazz.getName() + "." + field.getName())
                .collect(Collectors.toList());
        allFields.addAll(currentClassFields);

        //Get the uncapturedFields and baseState field name
        RestorableConfig config = getConfigAnnotation(clazz);
        String[] uncapturedFields = {};
        String baseClassStateName = "baseState";
        if (config != null) {
            uncapturedFields = config.uncapturedFields();
            baseClassStateName = config.baseClassStateName();
        }

        //If base class is Restorable and has any captured fields, the current class needs to have a field for base class state.
        if (clazz.getSuperclass() != null && Restorable.class.isAssignableFrom(clazz.getSuperclass())) {
            int baseFieldCount = test(clazz.getSuperclass());
            if (baseFieldCount > 0) {
                allFields.add(baseClassStateName);
            }
        }

        outer:
        for (String uncaptured : uncapturedFields) {
            int fragmentCount = uncaptured.split("\\.").length;
            if (fragmentCount > 2) {
                //Assumed to be "full-package-name.class.field"
                if (!allFields.remove(uncaptured)) {
                    unusedUncapturedFields.computeIfAbsent(clazz, ignored -> new HashSet<>()).add(uncaptured);
                }
            }
            else {
                //Partial name, "class.field" or just "field"
                //Try to look for a match in all base class fields from back to front.
                for (int i = allFields.size() - 1; i >= 0; i--) {
                    //$ before inner class names, . for others
                    if (allFields.get(i).endsWith("$" + uncaptured) || allFields.get(i).endsWith("." + uncaptured)) {
                        allFields.remove(i);
                        continue outer;
                    }
                }
                unusedUncapturedFields.computeIfAbsent(clazz, ignored -> new HashSet<>()).add(uncaptured);
            }
        }
    }

    private int test(Class<?> clazz)
            throws Exception
    {
        if (capturedFieldCount.containsKey(clazz)) {
            return capturedFieldCount.get(clazz);
        }

        RestorableConfig config = getConfigAnnotation(clazz);
        if (config != null && config.unsupported()) {
            capturedFieldCount.put(clazz, 0);
            return 0;
        }

        //Get all fields to be captured
        List<String> allFields = new ArrayList<>();
        getAllFields(allFields, clazz);
        //Convert to simple name
        allFields = allFields.stream().map(name -> {
            String[] splits = name.split("\\.");
            return splits[splits.length - 1];
        }).collect(Collectors.toList());
        Set<String> allFieldsSet = new HashSet<>(allFields);
        if (allFieldsSet.size() != allFields.size()) {
            failures.put(clazz, "Set size shrank, there are fields with duplicated name in base class(s).");
        }

        if (!containsCaptureMethod(clazz)) {
            // OK only if a single entry in allFields for the base state, and base has capture function
            String baseClassStateName = "baseState";
            if (config != null) {
                baseClassStateName = config.baseClassStateName();
            }
            if (allFields.size() == 1 && allFields.contains(baseClassStateName) && containsCaptureMethod(clazz.getSuperclass())) {
                // Return 1 to indicate that a subclass needs to include capture baseState
                capturedFieldCount.put(clazz, 1);
                return 1;
            }
            if (!allFields.isEmpty() || !Modifier.isAbstract(clazz.getModifiers())) {
                failures.put(clazz, "No capture function");
            }
        }

        String stateClassName = "";
        if (config != null) {
            stateClassName = config.stateClassName();
        }
        //Default State class name
        if (stateClassName.isEmpty()) {
            stateClassName = clazz.getSimpleName() + "State";
        }

        Class<?> stateClass = findStateClass(clazz, stateClassName);
        if (stateClass != null) {
            if (!Serializable.class.isAssignableFrom(stateClass)) {
                failures.put(clazz, "State class " + stateClass.getSimpleName() + " is doesn't implement Serializable.");
            }
            if (!Modifier.isStatic(stateClass.getModifiers())) {
                failures.put(clazz, "Inner State class is not static.");
            }

            Field[] snapshotFields = stateClass.getDeclaredFields();
            Set<String> snapshotFieldsName = Arrays.stream(snapshotFields).map(Field::getName).collect(Collectors.toSet());
            capturedFieldCount.put(clazz, snapshotFieldsName.size());
            Set<String> fieldsNotCovered = allFields.stream()
                    .filter(name -> !snapshotFieldsName.contains(name))
                    .filter(name -> !(name.contains("val$") && snapshotFieldsName.contains(name.substring(name.lastIndexOf("val$") + 4))))
                    .collect(Collectors.toSet());
            if (!fieldsNotCovered.isEmpty()) {
                failures.put(clazz, "Captured and Uncaptured fields doesn't include all fields.\n" + fieldsNotCovered);
            }

            if (config != null) {
                String[] uncapturedFields = config.uncapturedFields();
                Set<String> unusedUncaptured = Arrays.stream(uncapturedFields)
                        .filter(snapshotFieldsName::contains)
                        .collect(Collectors.toSet());
                if (!unusedUncaptured.isEmpty()) {
                    unusedUncapturedFields.computeIfAbsent(clazz, ignored -> new HashSet<>()).addAll(unusedUncaptured);
                }
            }
        }
        else {
            if (config != null) {
                if (!config.stateClassName().isEmpty()) {
                    failures.put(clazz, "Can't find specified state class " + config.stateClassName());
                }
            }
            int capturedCount = allFields.size();
            capturedFieldCount.put(clazz, capturedCount);
            if (capturedCount > 1) {
                failures.put(clazz, "No State class but has more than 1 field that needs capturing: " + allFields);
            }
        }

        return capturedFieldCount.get(clazz);
    }

    private Class<?> findStateClass(Class<?> clazz, String className)
    {
        while (clazz != null) {
            for (Class<?> inner : clazz.getDeclaredClasses()) {
                if (inner.getSimpleName().equals(className)) {
                    return inner;
                }
            }
            clazz = clazz.getName().contains("$") ? clazz.getEnclosingClass() : clazz.getDeclaringClass();
        }
        return null;
    }
}
