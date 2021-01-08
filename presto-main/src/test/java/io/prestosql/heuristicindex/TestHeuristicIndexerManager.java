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

package io.prestosql.heuristicindex;

import org.testng.annotations.Test;

import java.nio.file.FileSystemException;

import static io.prestosql.heuristicindex.HeuristicIndexerManager.checkFilesystemTimePrecision;

public class TestHeuristicIndexerManager
{
    @Test
    public void testFileSystemTimePrecisionMicro()
            throws FileSystemException
    {
        String timeStamp = "2021-01-12T11:41:51.036465Z";
        checkFilesystemTimePrecision(timeStamp);
    }

    @Test
    public void testFileSystemTimePrecisionMilli()
            throws FileSystemException
    {
        String timeStamp = "2021-01-12T11:41:51.036Z";
        checkFilesystemTimePrecision(timeStamp);
    }

    @Test(expectedExceptions = FileSystemException.class)
    public void testFileSystemTimePrecisionSec()
            throws FileSystemException
    {
        String timeStamp = "2021-01-12T11:41:51Z";
        checkFilesystemTimePrecision(timeStamp);
    }
}
