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
package io.prestosql.plugin.hive.metastore.file;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FileHiveMetastoreConfigTest
{
    private FileHiveMetastoreConfig fileHiveMetastoreConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        fileHiveMetastoreConfigUnderTest = new FileHiveMetastoreConfig();
    }

    @Test
    public void test()
    {
        String catalogDirectory = fileHiveMetastoreConfigUnderTest.getCatalogDirectory();
        fileHiveMetastoreConfigUnderTest.setCatalogDirectory(catalogDirectory);
        String metastoreUser = fileHiveMetastoreConfigUnderTest.getMetastoreUser();
        fileHiveMetastoreConfigUnderTest.setMetastoreUser(metastoreUser);
    }
}
