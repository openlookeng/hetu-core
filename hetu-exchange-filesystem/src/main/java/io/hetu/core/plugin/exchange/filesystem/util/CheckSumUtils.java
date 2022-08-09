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
package io.hetu.core.plugin.exchange.filesystem.util;

import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.lang.String.format;

public class CheckSumUtils
{
    private static final Logger LOG = Logger.get(CheckSumUtils.class);

    private CheckSumUtils()
    {
    }

    public static String checksum(String filepath) throws NoSuchAlgorithmException
    {
        return checksum(filepath, 4096, CheckSumAlgorithms.SHA256);
    }

    public static String checksum(String filepath, CheckSumAlgorithms algorithm) throws NoSuchAlgorithmException
    {
        return checksum(filepath, 4096, algorithm);
    }

    public static String checksum(String filepath, int bufferSize, CheckSumAlgorithms algorithm) throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance(algorithm.getAlgorithm());
        byte[] byteArray = new byte[bufferSize];
        int byteCount = 0;
        try (InputStream inputStream = Files.newInputStream(Paths.get(filepath))) {
            while ((byteCount = inputStream.read(byteArray)) != -1) {
                digest.update(byteArray, 0, byteCount);
            }
        }
        catch (IOException e) {
            LOG.info(e.getMessage());
        }

        StringBuilder result = new StringBuilder();
        for (byte b : digest.digest()) {
            result.append(format("%02x", b));
        }
        return result.toString();
    }

    /**
     * generate checksum for byte array using SHA256 algorithm
     *
     * @param bytes data
     * @return checksum string
     * @throws NoSuchAlgorithmException algorithm not supported
     */
    public static String checksum(byte[] bytes) throws NoSuchAlgorithmException
    {
        return checksum(bytes, CheckSumAlgorithms.SHA256);
    }

    /**
     * generate checksum for byte array using passed in algorithm
     *
     * @param bytes data
     * @return checksum string
     * @throws NoSuchAlgorithmException algorithm not supported
     */
    public static String checksum(byte[] bytes, CheckSumAlgorithms algorithm) throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance(algorithm.getAlgorithm());
        StringBuilder result = new StringBuilder();
        for (byte b : digest.digest(bytes)) {
            result.append(format("%02x", b));
        }
        return result.toString();
    }

    public enum CheckSumAlgorithms
    {
        SHA256("SHA-256"), SHA512("SHA-512"), MD5("MD5");

        private final String algorithm;

        CheckSumAlgorithms(String algorithm)
        {
            this.algorithm = algorithm;
        }

        public String getAlgorithm()
        {
            return algorithm;
        }
    }
}
