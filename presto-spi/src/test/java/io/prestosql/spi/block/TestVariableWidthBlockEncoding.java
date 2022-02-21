/*
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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Stopwatch;
import io.airlift.slice.DynamicSliceOutput;
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
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestVariableWidthBlockEncoding
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
    private String storePath = "./target/store";

    private Kryo kryo;
    private Output output;
    private Input input;

    @BeforeClass
    public void init()
    {
        kryo = new Kryo();
        kryo.register(VariableWidthBlock.class);

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
    public void testRoundTrip()
    {
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
    }

    @Test
    public void testRoundTripKryo() throws FileNotFoundException
    {
        output = new Output(new FileOutputStream(storePath + "/" + "file.dat"));
        input = new Input(new FileInputStream(storePath + "/" + "file.dat"));

        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        kryo.writeObject(output, expectedBlock);
        output.close();

        Block actualBlock = kryo.readObject(input, VariableWidthBlock.class);
        assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
        input.close();
    }

    public void testRoundTripKryoPerf100000000() throws IOException
    {
        int loopCount = 1000;
        for (int i = 1; i <= 5; i++) {
            loopCount *= 10;
            loopReadWritePerfTest(loopCount);
        }
    }

    private void loopReadWritePerfTest(int loopCount) throws IOException
    {
        output = new Output(new FileOutputStream(storePath + "/" + "file.dat"));
        input = new Input(new FileInputStream(storePath + "/" + "file.dat"));

        OutputStreamSliceOutput sliceOutput = new OutputStreamSliceOutput(new FileOutputStream(storePath + "/" + "sliceFile.dat"));
        InputStreamSliceInput sliceInput = new InputStreamSliceInput(new FileInputStream(storePath + "/" + "sliceFile.dat"));

        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        Stopwatch watchKryoWrite = Stopwatch.createStarted();
        for (int i = 0; i < loopCount; i++) {
            kryo.writeObject(output, expectedBlock);
        }
        watchKryoWrite.stop();
        System.out.println(String.format("[Pages: %,11d] Time to write       [Kryo]: %,7d ms", loopCount, watchKryoWrite.elapsed(TimeUnit.MILLISECONDS)));
        output.close();

        Stopwatch watchSerDeWrite = Stopwatch.createStarted();
        for (int i = 0; i < loopCount; i++) {
            blockEncodingSerde.writeBlock(sliceOutput, expectedBlock);
        }
        watchSerDeWrite.stop();
        System.out.println(String.format("[Pages: %,11d] Time to write [BlockSerDe]: %,7d ms", loopCount, watchSerDeWrite.elapsed(TimeUnit.MILLISECONDS)));
        sliceOutput.close();

        Stopwatch watchKryoRead = Stopwatch.createStarted();
        for (int i = 0; i < loopCount; i++) {
            Block actualBlock = kryo.readObject(input, VariableWidthBlock.class);
            assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
        }
        watchKryoRead.stop();
        System.out.println(String.format("[Pages: %,11d] Time to read        [Kryo]: %,7d ms", loopCount, watchKryoRead.elapsed(TimeUnit.MILLISECONDS)));
        input.close();

        Stopwatch watchSerDeRead = Stopwatch.createStarted();
        for (int i = 0; i < loopCount; i++) {
            Block actualBlock = blockEncodingSerde.readBlock(sliceInput);
            assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
        }
        watchSerDeRead.stop();
        System.out.println(String.format("[Pages: %,11d] Time to read  [BlockSerDe]: %,7d ms", loopCount, watchSerDeRead.elapsed(TimeUnit.MILLISECONDS)));
        input.close();
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
