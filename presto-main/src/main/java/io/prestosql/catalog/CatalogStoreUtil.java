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

package io.prestosql.catalog;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.prestosql.security.CipherTextDecryptUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.prestosql.spi.HetuConstant.ENCRYPTED_PROPERTIES;

public class CatalogStoreUtil
{
    private CipherTextDecryptUtil cipherTextDecryptUtil;

    @Inject
    private CatalogStoreUtil(CipherTextDecryptUtil cipherTextDecryptUtil)
    {
        this.cipherTextDecryptUtil = cipherTextDecryptUtil;
    }

    private Set<String> splitEncryptedProperties(String encryptedPropertyNamesValue)
    {
        if (encryptedPropertyNamesValue == null || encryptedPropertyNamesValue.isEmpty()) {
            return ImmutableSet.of();
        }
        String[] encryptedPropertyNameArray = encryptedPropertyNamesValue.split(",");
        Set<String> encryptedPropertyNames = new HashSet<>();
        for (String encryptedPropertyName : encryptedPropertyNameArray) {
            encryptedPropertyNames.add(encryptedPropertyName.trim());
        }
        return encryptedPropertyNames;
    }

    /**
     * Decrypt the encrypted properties, and
     * "encrypted.properties"
     *
     * @param decryptKeyName the key name.
     * @param properties the properties of catalog.
     */
    public void decryptEncryptedProperties(String decryptKeyName, Map<String, String> properties)
    {
        String encryptedPropertiesValue = properties.remove(ENCRYPTED_PROPERTIES);
        Set<String> encryptedProperties = splitEncryptedProperties(encryptedPropertiesValue);
        encryptedProperties.forEach(propertyName -> {
            String cipherText = properties.get(propertyName);
            if (cipherText != null) {
                String plainText = cipherTextDecryptUtil.decrypt(decryptKeyName, cipherText);
                properties.put(propertyName, plainText.replaceAll("\n", ""));
            }
        });
    }
}
