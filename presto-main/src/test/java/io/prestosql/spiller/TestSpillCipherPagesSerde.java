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
package io.prestosql.spiller;

import com.google.common.collect.ImmutableList;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.spiller.SpillCipher;
import io.prestosql.spi.type.Type;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingPagesSerdeFactory.TESTING_SERDE_FACTORY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestSpillCipherPagesSerde
{
    @Test
    public void test()
    {
        SpillCipher cipher = new AesSpillCipher();
        PagesSerde serde = TESTING_SERDE_FACTORY.createPagesSerdeForSpill(Optional.of(cipher), false);
        List<Type> types = ImmutableList.of(VARCHAR);
        Page emptyPage = new Page(VARCHAR.createBlockBuilder(null, 0).build());
        assertPageEquals(types, serde.deserialize(serde.serialize(emptyPage)), emptyPage);

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 2);
        VARCHAR.writeString(blockBuilder, "hello");
        VARCHAR.writeString(blockBuilder, "world");
        Page helloWorldPage = new Page(blockBuilder.build());

        SerializedPage serialized = serde.serialize(helloWorldPage);
        assertPageEquals(types, serde.deserialize(serialized), helloWorldPage);
        assertTrue(serialized.isEncrypted(), "page should be encrypted");

        cipher.close();

        assertFailure(() -> serde.serialize(helloWorldPage), "Spill cipher already closed");
        assertFailure(() -> serde.deserialize(serialized), "Spill cipher already closed");
    }

    private static void assertFailure(ThrowingRunnable runnable, String expectedErrorMessage)
    {
        PrestoException exception = expectThrows(PrestoException.class, runnable);
        assertEquals(exception.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertEquals(exception.getMessage(), expectedErrorMessage);
    }
}
