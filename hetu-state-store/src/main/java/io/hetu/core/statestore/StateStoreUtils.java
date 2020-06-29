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
package io.hetu.core.statestore;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.statestore.CipherService;

import java.util.Map;

import static io.hetu.core.statestore.Constants.STATE_STORE_ENCRYPTION_CONFIG_NAME;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;

/**
 * Utils class for StateStore
 *
 * @since 2020-03-20
 */
public class StateStoreUtils
{
    private StateStoreUtils()
    {
    }

    /**
     * Get encryption type from StateStore config
     *
     * @param properties StateStore config
     * @return encryption type
     */
    public static CipherService.Type getEncryptionTypeFromConfig(Map<String, String> properties)
    {
        String encryptionTypeString = properties.get(STATE_STORE_ENCRYPTION_CONFIG_NAME);

        if (encryptionTypeString == null) {
            return CipherService.Type.NONE;
        }

        switch (encryptionTypeString) {
            case "none":
                return CipherService.Type.NONE;
            case "base64encoding":
                return CipherService.Type.BASE64;
            default:
                throw new PrestoException(CONFIGURATION_INVALID,
                        "Unsupported encryption type: " + encryptionTypeString);
        }
    }
}
