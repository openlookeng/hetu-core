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
package io.hetu.core.common.filesystem;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class TempFolderTest
{
    private TempFolder tempFolderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        tempFolderUnderTest = new TempFolder("user");
        TempFolder tempFolder = new TempFolder();
    }

    @Test
    public void testCreate() throws Exception
    {
        // Setup
        // Run the test
        final TempFolder result = tempFolderUnderTest.create();

        // Verify the results
    }

    @Test
    public void testCreate_ThrowsIOException() throws IOException
    {
        // Setup
        // Run the test
        tempFolderUnderTest.create();
    }

    @Test
    public void testGetRoot()
    {
        // Setup
        // Run the test
        File root = tempFolderUnderTest.getRoot();
    }

    @Test
    public void testNewFile1() throws Exception
    {
        // Setup
        final File expectedResult = new File("filename.txt");

        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        final File result = tempFolderUnderTest.newFile();
    }

    @Test
    public void testNewFile1_ThrowsIOException() throws IOException
    {
        // Setup
        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        File file = tempFolderUnderTest.newFile();
    }

    @Test
    public void testNewFile2() throws Exception
    {
        // Setup
        final File expectedResult = new File("/filename.txt");

        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        final File result = tempFolderUnderTest.newFile("relativePath");
    }

    @Test
    public void testNewFile2_ThrowsIOException() throws IOException
    {
        // Setup
        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        File relativePath = tempFolderUnderTest.newFile("relativePath");
    }

    @Test
    public void testNewFolder1() throws Exception
    {
        // Setup
        final File expectedResult = new File("filename.txt");

        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        final File result = tempFolderUnderTest.newFolder();
    }

    @Test
    public void testNewFolder1_ThrowsIOException() throws IOException
    {
        // Setup
        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        File file = tempFolderUnderTest.newFolder();
    }

    @Test
    public void testNewFolder2() throws Exception
    {
        // Setup
        final File expectedResult = new File("filename.txt");

        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        final File result = tempFolderUnderTest.newFolder("relativePath");
    }

    @Test
    public void testNewFolder2_ThrowsIOException() throws IOException
    {
        // Setup
        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        File relativePath = tempFolderUnderTest.newFolder("relativePath");
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        tempFolderUnderTest.close();

        // Verify the results
    }

    @Test
    public void testClose2() throws Exception
    {
        // Setup
        // Run the test
        TempFolder tempFolder = tempFolderUnderTest.create();
        tempFolderUnderTest.close();

        // Verify the results
    }

    @Test
    public void testClose_ThrowsException()
    {
        // Setup
        // Run the test
        tempFolderUnderTest.close();
    }
}
