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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Help to clarify snapshot support of the annotated class:
 * - unsupported: controls whether the class is expeccted to be involved in snapshot. Default is false.
 * - stateClassName: name of state class, the result of capturing the annotated class. Default is class name with State suffix.
 * The state class can also be omitted if there are 0 or 1 fields to capture.
 * - baseClassStateName: name of field in stateClass that corresponds to result of "super.capture()". Default is "baseState".
 * - uncapturedFields: which fields don't need to be captured.
 * <p>
 * A special case is for anonymous classes, where it's not easy to attach this annotation.
 * new @RestorableConfig(...) SomeInterface {
 * int a;
 * ...
 * }
 * This works syntactically, but a Java issue sometimes causes the field "a" to not be accessible by methods in the class.
 * To work around it, for anonymous classes, instead of annotating the class, we allow:
 * new SomeInterface {
 *
 * @RestorableConfig(...) private final RestorableConfig restorableConfig = null;
 * int a;
 * ...
 * }
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
public @interface RestorableConfig
{
    boolean unsupported() default false;

    String stateClassName() default "";

    String baseClassStateName() default "baseState";

    String[] uncapturedFields() default {};
}
