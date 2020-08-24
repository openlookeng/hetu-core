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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.spi.security.CipherTextDecrypt;
import io.prestosql.spi.security.SecurityKeyManager;

import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class PasswordSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Key Manager
        install(installModuleIf(
                PasswordSecurityConfig.class,
                config -> "keystore".equalsIgnoreCase(config.getKeyManagerType()),
                moduleBinder -> moduleBinder.bind(SecurityKeyManager.class).to(KeystoreSecurityKeyManager.class).in(Scopes.SINGLETON),
                moduleBinder -> moduleBinder.bind(SecurityKeyManager.class).to(NoneSecurityKeyManager.class).in(Scopes.SINGLETON)));

        // Cipher text decryption
        binder.bind(CipherTextDecryptUtil.class).in(Scopes.SINGLETON);
        install(installModuleIf(
                PasswordSecurityConfig.class,
                config -> "RSA".equalsIgnoreCase(config.getDecryptionType()),
                moduleBinder -> moduleBinder.bind(CipherTextDecrypt.class).to(RsaCipherTextDecrypt.class).in(Scopes.SINGLETON),
                moduleBinder -> moduleBinder.bind(CipherTextDecrypt.class).to(NoneCipherTextDecrypt.class).in(Scopes.SINGLETON)));
    }
}
