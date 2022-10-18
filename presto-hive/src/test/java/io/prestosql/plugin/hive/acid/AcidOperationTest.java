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

import org.testng.annotations.Test;

public class AcidOperationTest
{
    @Test
    public void testGetMetastoreOperationType()
    {
        AcidOperation.NONE.getMetastoreOperationType();
        AcidOperation.CREATE_TABLE.getMetastoreOperationType();
        AcidOperation.DELETE.getMetastoreOperationType();
        AcidOperation.INSERT.getMetastoreOperationType();
        AcidOperation.UPDATE.getMetastoreOperationType();
    }

    @Test
    public void testGetOrcOperation()
    {
        AcidOperation.NONE.getOrcOperation();
        AcidOperation.CREATE_TABLE.getOrcOperation();
        AcidOperation.DELETE.getOrcOperation();
        AcidOperation.INSERT.getOrcOperation();
        AcidOperation.UPDATE.getOrcOperation();
    }
}
