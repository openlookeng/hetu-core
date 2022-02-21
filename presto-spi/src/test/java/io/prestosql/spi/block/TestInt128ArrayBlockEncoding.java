/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.spi.block;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestInt128ArrayBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
    private String storePath = "./target/store";

    @BeforeClass
    public void init()
    {
        File dir = new File(storePath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    @AfterClass
    public void teardown()
    {
        File dir = new File(storePath);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                file.delete();
            }
            dir.delete();
        }
    }

    @Test
    public void testRoundTrip() throws IOException
    {
        int count = 1024;
        Int128ArrayBlock expectedBlock = new Int128ArrayBlock(count, Optional.empty(), getValues(count * 2));

        OutputStreamSliceOutput sliceOutput = new OutputStreamSliceOutput(new FileOutputStream(storePath + "/" + "sliceFile.dat"));
        InputStreamSliceInput sliceInput = new InputStreamSliceInput(new FileInputStream(storePath + "/" + "sliceFile.dat"));

        blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        sliceOutput.close();

        Block actualBlock = blockEncodingSerde.readBlock(sliceInput);
        sliceInput.close();
        assertBlockEquals(BIGINT, actualBlock, expectedBlock);
    }

    @Test
    public void testRoundTripDirect() throws FileNotFoundException
    {
        int count = 1024;
        Int128ArrayBlock expectedBlock = new Int128ArrayBlock(count, Optional.empty(), getValues(count * 2));
        Output output = new Output(new FileOutputStream(storePath + "/" + "file.dat"));
        Input input = new Input(new FileInputStream(storePath + "/" + "file.dat"));

        blockEncodingSerde.writeBlock(output, expectedBlock);
        output.close();

        Block actualBlock = blockEncodingSerde.readBlock(input);
        input.close();

        assertBlockEquals(BIGINT, actualBlock, expectedBlock);
    }

    private long[] getValues(int count)
    {
        long[] values = new long[count];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        return values;
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
