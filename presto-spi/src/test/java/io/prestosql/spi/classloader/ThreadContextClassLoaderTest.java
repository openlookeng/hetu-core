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
package io.prestosql.spi.classloader;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertThrows;

public class ThreadContextClassLoaderTest
{
    @Mock
    private ClassLoader mockNewThreadContextClassLoader;

    private ThreadContextClassLoader threadContextClassLoaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        threadContextClassLoaderUnderTest = new ThreadContextClassLoader(mockNewThreadContextClassLoader);
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        threadContextClassLoaderUnderTest.close();

        // Verify the results
    }

    @Test
    public void testClose_ThrowsIOException()
    {
        // Setup
        // Run the test
        assertThrows(IOException.class, () -> threadContextClassLoaderUnderTest.close());
    }
}
