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
package io.prestosql.plugin.hive.acid;

import io.prestosql.orc.OrcWriter;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveUpdateProcessor;
import io.prestosql.plugin.hive.WriterKind;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;

public class AcidTransactionTest
{
    private AcidTransaction acidTransactionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        acidTransactionUnderTest = new AcidTransaction(AcidOperation.INSERT, 0L, 0L, Optional.of(new HiveUpdateProcessor(
                Arrays.asList(new HiveColumnHandle("name", HIVE_STRING,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)), Arrays.asList(
                                new HiveColumnHandle("name", HIVE_STRING,
                                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)))));
    }

    @Test
    public void testGetAcidTransactionIdForSerialization()
    {
        acidTransactionUnderTest.getAcidTransactionIdForSerialization();
    }

    @Test
    public void testGetWriteIdForSerialization()
    {
        acidTransactionUnderTest.getWriteIdForSerialization();
    }

    @Test
    public void testIsAcidTransactionRunning()
    {
        acidTransactionUnderTest.isAcidTransactionRunning();
    }

    @Test
    public void testIsTransactional()
    {
        acidTransactionUnderTest.isTransactional();
    }

    @Test
    public void testGetOrcOperation()
    {
        // Setup
        // Run the test
        final Optional<OrcWriter.OrcOperation> result = acidTransactionUnderTest.getOrcOperation();
    }

    @Test
    public void testGetAcidTransactionId()
    {
        acidTransactionUnderTest.getAcidTransactionId();
    }

    @Test
    public void testIsInsert()
    {
        acidTransactionUnderTest.isInsert();
    }

    @Test
    public void testIsDelete()
    {
        acidTransactionUnderTest.isDelete();
    }

    @Test
    public void testIsUpdate()
    {
        acidTransactionUnderTest.isUpdate();
    }

    @Test
    public void testIsAcidInsertOperation()
    {
        acidTransactionUnderTest.isAcidInsertOperation(WriterKind.DELETE);
    }

    @Test
    public void testIsAcidDeleteOperation()
    {
        acidTransactionUnderTest.isAcidDeleteOperation(WriterKind.DELETE);
    }

    @Test
    public void testToString() throws Exception
    {
        acidTransactionUnderTest.toString();
    }

    @Test
    public void testForCreateTable()
    {
        // Run the test
        final AcidTransaction result = AcidTransaction.forCreateTable();
        acidTransactionUnderTest.getOperation();
        acidTransactionUnderTest.getUpdateProcessor();
        acidTransactionUnderTest.getWriteId();
    }
}
