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
package io.prestosql.plugin.hive.s3;

import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.SimpleMaterialProvider;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrestoS3Constants
{
    /**
     * EncryptionMaterialsProvider Implementation List
     */
    public static final List<String> ENCRYPTIONMATERIALSPROVIDER_IMPL_LIST = Collections.unmodifiableList(new ArrayList<String>() {
        {
            this.add("io.prestosql.plugin.hive.s3.TestPrestoS3FileSystem$TestEncryptionMaterialsProvider");
            this.add(KMSEncryptionMaterialsProvider.class.getName());
            this.add(SimpleMaterialProvider.class.getName());
            this.add(StaticEncryptionMaterialsProvider.class.getName());
        }
    });

    private PrestoS3Constants()
    {
    }
}
