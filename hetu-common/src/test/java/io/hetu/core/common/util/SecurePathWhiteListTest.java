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
package io.hetu.core.common.util;

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class SecurePathWhiteListTest
{
    @Test
    public void testGetSecurePathWhiteList() throws Exception
    {
        SecurePathWhiteList.getSecurePathWhiteList();
    }

    @Test
    public void testIsSecurePath1() throws Exception
    {
        assertTrue(SecurePathWhiteList.isSecurePath(Paths.get("filename.txt")));
        assertThrows(IOException.class, () -> SecurePathWhiteList.isSecurePath(Paths.get("filename.txt")));
    }

    @Test
    public void testIsSecurePath2() throws Exception
    {
        assertTrue(SecurePathWhiteList.isSecurePath("/absolutePath"));
        assertThrows(IOException.class, () -> SecurePathWhiteList.isSecurePath("/absolutePath"));
    }
}
