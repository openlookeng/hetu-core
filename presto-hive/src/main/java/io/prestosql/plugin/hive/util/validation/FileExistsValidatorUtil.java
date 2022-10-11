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

import com.google.common.base.Verify;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileExistsValidatorUtil
        implements ConstraintValidator<FileExistsUtil, Object>
{
    public FileExistsValidatorUtil()
    {
    }

    @Override
    public void initialize(FileExistsUtil ignored)
    {
        Verify.verify(ignored.message().isEmpty(), "FileExists.message cannot be specified", new Object[0]);
    }

    @Override
    public boolean isValid(Object path, ConstraintValidatorContext context)
    {
        if (path == null) {
            return true;
        }
        else {
            boolean fileExists = exists(path);
            if (!fileExists) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate("file does not exist: " + path).addConstraintViolation();
            }

            return fileExists;
        }
    }

    private static boolean exists(Object path)
    {
        if (path instanceof String) {
            return Files.exists(Paths.get((String) path), new LinkOption[0]);
        }
        else if (path instanceof Path) {
            return Files.exists((Path) path, new LinkOption[0]);
        }
        else if (path instanceof File) {
            return ((File) path).exists();
        }
        else {
            throw new IllegalArgumentException("Unsupported type for @FileExists: " + path.getClass().getName());
        }
    }
}
