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

package io.hetu.core.heuristicindex.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Util class for creating Hive external index.
 */
public class HiveIndexServiceUtil
{
    private HiveIndexServiceUtil()
    {
    }

    /**
     * get an Kerberos Token for OrcReader
     *
     * @param conf           conf env built from xml files
     * @param krb5FilePath   krb5 files for Kerbros KDC
     * @param keytabFilePath keytab for Kerbros user
     * @param principle      principle for Kerbros user
     * @throws IOException when fails
     */
    public static synchronized void getKerberosToken(Configuration conf, String krb5FilePath, String keytabFilePath,
                                                     String principle) throws IOException
    {
        System.setProperty(ConstantsHelper.KRB5_CONF_KEY, krb5FilePath);

        // In order to enable KERBEROS authentication method for HDFS
        // UserGroupInformation.authenticationMethod static field must be set to KERBEROS
        // It is further used in many places in DfsClient
        conf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principle, keytabFilePath);
    }
}
