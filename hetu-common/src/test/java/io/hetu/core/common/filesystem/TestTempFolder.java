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
package io.hetu.core.common.filesystem;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTempFolder
{
    @Test
    public void testFolder()
            throws IOException
    {
        File root;
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            root = folder.getRoot();
            assertTrue(root.exists());
            File newFile = folder.newFile("aNewFile");
            assertEquals(newFile.getAbsolutePath(), folder.getRoot().getAbsolutePath() + "/aNewFile");
            File newFolder = folder.newFile("aNewFolder");
            assertEquals(newFolder.getAbsolutePath(), folder.getRoot().getAbsolutePath() + "/aNewFolder");
        }
        assertFalse(root.exists());
    }
}
