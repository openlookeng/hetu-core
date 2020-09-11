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
package io.prestosql.plugin.hive.security;

import io.prestosql.plugin.base.security.AllowAllAccessControl;
import io.prestosql.plugin.base.security.FileBasedAccessControl;
import io.prestosql.plugin.base.security.ForwardingConnectorAccessControl;
import io.prestosql.plugin.base.security.ReadOnlyAccessControl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SecurityConstants
{
    /**
     * SqlStandardAccessControl implementation white list
     */
    public static final List<String> WHITE_LIST_SQLSTANDARDACCESSCONTROL_IMPL = Collections.unmodifiableList(new ArrayList<String>() {
        {
            // for a full name of class string will cause a maven-dependency-plugin issue, we need to separate it into two string
            String classPackage = "io.prestosql.security";
            String className = ".TestAccessControlManager$DenyConnectorAccessControl";
            this.add(classPackage + className);
            this.add(AllowAllAccessControl.class.getName());
            this.add(ForwardingConnectorAccessControl.class.getName());
            this.add(FileBasedAccessControl.class.getName());
            this.add(LegacyAccessControl.class.getName());
            this.add(ReadOnlyAccessControl.class.getName());
            this.add(SqlStandardAccessControl.class.getName());
            this.add(SystemTableAwareAccessControl.class.getName());
        }
    });

    private SecurityConstants()
    {
    }
}
