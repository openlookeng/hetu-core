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
package io.prestosql.plugin.hive.util.validation;

import io.prestosql.plugin.hive.HiveConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import java.io.File;
import java.lang.annotation.Annotation;

public class FileExistsValidatorUtilTest
{
    private FileExistsValidatorUtil fileExistsValidatorUtilUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        fileExistsValidatorUtilUnderTest = new FileExistsValidatorUtil();
    }

    public static class FileExistsUtil
            implements io.prestosql.plugin.hive.util.validation.FileExistsUtil
    {
        @Override
        public String message()
        {
            return "";
        }

        @Override
        public Class<?>[] groups()
        {
            return new Class[0];
        }

        @Override
        public Class<? extends Payload>[] payload()
        {
            return new Class[0];
        }

        @Override
        public Class<? extends Annotation> annotationType()
        {
            return null;
        }
    }

    @Test
    public void testInitialize() throws Exception
    {
        fileExistsValidatorUtilUnderTest.initialize(new FileExistsUtil());

        // Verify the results
    }

    @Test
    public void testIsValid()
    {
        // Setup
        final ConstraintValidatorContext context = null;

        // Run the test
        fileExistsValidatorUtilUnderTest.isValid(null, context);
        fileExistsValidatorUtilUnderTest.isValid("path", context);
    }

    @Test
    public void testIsValid2()
    {
        // Setup
        final ConstraintValidatorContext context = null;

        // Run the test
        fileExistsValidatorUtilUnderTest.isValid(new File("/file"), context);
    }

    @Test
    public void testIsValid3()
    {
        // Setup
        final ConstraintValidatorContext context = null;

        // Run the test
        fileExistsValidatorUtilUnderTest.isValid(new HiveConfig(), context);
    }
}
