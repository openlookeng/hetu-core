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
package io.hetu.core.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public final class SecureObjectInputStream
        extends ObjectInputStream
{
    private String[] acceptedClasses;

    /**
     * Creates an ObjectInputStream which only allows reading objects
     * specified in acceptedClasses.
     *
     * SecureObjectInputStream#readObject will throw a ClassNotFoundException
     * if the class read by readObject() is not one of the accepted classes.
     *
     * @param in input stream
     * @param acceptedClasses list of accepted classes, e.g. java.lang.String
     * @throws IOException
     */
    public SecureObjectInputStream(InputStream in, String... acceptedClasses) throws IOException
    {
        super(in);
        this.acceptedClasses = acceptedClasses;
    }

    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException
    {
        if (acceptedClasses != null && acceptedClasses.length != 0) {
            boolean matchFound = false;
            String className = desc.getName();
            for (String acceptedClass : acceptedClasses) {
                if (className.equals(acceptedClass)) {
                    matchFound = true;
                    break;
                }
            }

            if (!matchFound) {
                throw new InvalidClassException(desc.getName() + " not supported.");
            }
        }

        return super.resolveClass(desc);
    }
}
