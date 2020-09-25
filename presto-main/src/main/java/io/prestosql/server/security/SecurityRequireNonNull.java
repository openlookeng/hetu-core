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
package io.prestosql.server.security;

import java.util.function.Supplier;

public class SecurityRequireNonNull
{
    private SecurityRequireNonNull()
    {
    }

    /**
     * security requireNonNull
     *
     * @param var variable
     * @throws NullPointerException
     */
    public static <T> T requireNonNull(T var)
    {
        if (var == null) {
            throw new NullPointerException()
            {
                @Override
                public synchronized Throwable fillInStackTrace()
                {
                    return this;
                }
            };
        }
        else {
            return var;
        }
    }

    /**
     * security requireNonNull
     *
     * @param varT variable
     * @param varStr message
     * @throws NullPointerException
     */
    public static <T> T requireNonNull(T varT, String varStr)
    {
        if (varT == null) {
            throw new NullPointerException(varStr)
            {
                @Override
                public synchronized Throwable fillInStackTrace()
                {
                    return this;
                }
            };
        }
        else {
            return varT;
        }
    }

    /**
     * security requireNonNull
     *
     * @param varT variable
     * @param stringSupplier message supplier
     * @throws NullPointerException
     */
    public static <T> T requireNonNull(T varT, Supplier<String> stringSupplier)
    {
        if (varT == null) {
            throw new NullPointerException((String) stringSupplier.get())
            {
                @Override
                public synchronized Throwable fillInStackTrace()
                {
                    return this;
                }
            };
        }
        else {
            return varT;
        }
    }
}
