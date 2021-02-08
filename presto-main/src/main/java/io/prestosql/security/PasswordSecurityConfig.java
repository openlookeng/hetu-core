/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.security;

import io.airlift.configuration.Config;

import java.io.File;

public class PasswordSecurityConfig
{
    private String decryptionType = "NONE";
    private String keyManagerType = "keystore";
    private String keystorePassword;
    private String fileStorePath = File.separator + "catalogs" + File.separator + "keys" + File.separator + "keystore.jks";
    private String rsaPadding = "RSA/ECB/OAEPWITHSHA256AndMGF1Padding";
    private String shareFileSystemProfile = "hdfs-config-default";

    public String getDecryptionType()
    {
        return decryptionType;
    }

    @Config("security.share.filesystem.profile")
    public PasswordSecurityConfig setShareFileSystemProfile(String shareFileSystemProfile)
    {
        this.shareFileSystemProfile = shareFileSystemProfile;
        return this;
    }

    public String getShareFileSystemProfile()
    {
        return shareFileSystemProfile;
    }

    @Config("security.password.decryption-type")
    public PasswordSecurityConfig setDecryptionType(String decryptionType)
    {
        this.decryptionType = decryptionType;
        return this;
    }

    public String getKeyManagerType()
    {
        return keyManagerType;
    }

    @Config("security.key.manager-type")
    public PasswordSecurityConfig setKeyManagerType(String keyManagerType)
    {
        this.keyManagerType = keyManagerType;
        return this;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("security.key.keystore-password")
    public PasswordSecurityConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public String getFileStorePath()
    {
        return fileStorePath;
    }

    @Config("security.key.store-file-path")
    public PasswordSecurityConfig setFileStorePath(String fileStorePath)
    {
        this.fileStorePath = fileStorePath;
        return this;
    }

    public String getRsaPadding()
    {
        return rsaPadding;
    }

    @Config("security.key.cipher-transformations")
    public void setRsaPadding(String rsaPadding)
    {
        this.rsaPadding = rsaPadding;
    }
}
