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
package io.hetu.core.plugin.hbase.security;

import io.airlift.log.Logger;
import io.hetu.core.common.util.SecurePathWhiteList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;

import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * HBase Kerberos Authentication
 *
 * @since 2020-03-18
 */
public class HBaseKerberosAuthentication
{
    private static final Logger LOG = Logger.get(HBaseKerberosAuthentication.class);

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final boolean IS_IBM_JDK = (System.getProperty("java.vendor") != null) && System.getProperty("java.vendor").contains("IBM");

    private HBaseKerberosAuthentication() {}

    /**
     * authenticate
     *
     * @param userPrincipal userPrincipal
     * @param userKeytabPath userKeytabPath
     * @param krb5ConfPath krb5ConfPath
     * @param conf conf
     * @return UserGroupInformation
     * @throws IOException IOException
     */
    public static synchronized UserGroupInformation authenticateAndReturnUGI(
            String userPrincipal, String userKeytabPath, String krb5ConfPath, Configuration conf)
            throws IOException
    {
        // 1.check input parameters
        if ((userPrincipal == null) || (userPrincipal.length() <= 0)) {
            throw new IOException("input userPrincipal is invalid.");
        }

        if ((userKeytabPath == null) || (userKeytabPath.length() <= 0)) {
            throw new IOException("input userKeytabPath is invalid.");
        }

        if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0)) {
            throw new IOException("input krb5ConfPath is invalid.");
        }

        if ((conf == null)) {
            throw new IOException("input conf is invalid.");
        }

        // check file in white path list
        try {
            checkArgument(!userKeytabPath.contains("../"),
                    userKeytabPath + "Path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(userKeytabPath),
                    userKeytabPath + "Path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(!krb5ConfPath.contains("../"),
                    krb5ConfPath + "Path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(krb5ConfPath),
                    krb5ConfPath + "Path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }

        // 2.check file exsits
        File userKeytabFile = new File(userKeytabPath);
        if (!userKeytabFile.exists()) {
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile()) {
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") is not a file.");
        }

        File krb5ConfFile = new File(krb5ConfPath);
        if (!krb5ConfFile.exists()) {
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") does not exsit.");
        }
        if (!krb5ConfFile.isFile()) {
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") is not a file.");
        }

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getCanonicalPath());
        UserGroupInformation.setConfiguration(conf);

        // 4.login and check for hadoop
        UserGroupInformation.loginUserFromKeytab(userPrincipal, userKeytabFile.getCanonicalPath());
        LOG.info("Login success!");
        return UserGroupInformation.getCurrentUser();
    }

    /**
     * krb5 file set
     *
     * @param krb5ConfFile krb5ConfFile
     * @throws IOException IOException
     */
    public static void setKrb5Config(String krb5ConfFile)
            throws IOException
    {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }

    /**
     * jass file set
     *
     * @param loginContextName loginContextName
     * @param principal principal name
     * @param keytabFile keytab File
     * @throws IOException IOException
     */
    public static void setJaasConf(String loginContextName, String principal, String keytabFile)
            throws IOException
    {
        checkFileValid(loginContextName);
        checkFileValid(principal);
        checkFileValid(keytabFile);

        File userKeytabFile = new File(keytabFile);
        if (!userKeytabFile.exists()) {
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
        }

        javax.security.auth.login.Configuration.setConfiguration(
                new JaasConfiguration(loginContextName, principal, userKeytabFile.getCanonicalPath()));

        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof JaasConfiguration)) {
            throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }

        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
        if (entrys == null) {
            throw new IOException(
                    "javax.security.auth.login.Configuration has no AppConfigurationEntry named "
                            + loginContextName
                            + ".");
        }

        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (int i = 0; i < entrys.length; i++) {
            if (entrys[i].getOptions().get("principal").equals(principal)) {
                checkPrincipal = true;
            }

            if (IS_IBM_JDK) {
                if (entrys[i].getOptions().get("useKeytab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            }
            else {
                if (entrys[i].getOptions().get("keyTab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            }
        }

        checkKerberosValid(checkPrincipal, "principal", loginContextName);
        checkKerberosValid(checkKeytab, "keyTab", loginContextName);
    }

    private static void checkFileValid(String fileName)
            throws IOException
    {
        if ((fileName == null) || (fileName.length() <= 0)) {
            throw new IOException("input" + fileName + " is invalid.");
        }
    }

    private static void checkKerberosValid(boolean check, String fileName, String loginContextName)
            throws IOException
    {
        if (!check) {
            throw new IOException(
                    "AppConfigurationEntry named "
                            + loginContextName
                            + " does not have value of "
                            + fileName
                            + ".");
        }
    }

    private static class JaasConfiguration
            extends javax.security.auth.login.Configuration
    {
        private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap();

        static {
            String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
            if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
                KEYTAB_KERBEROS_OPTIONS.put("debug", "true");
            }
        }

        static {
            if (IS_IBM_JDK) {
                KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
            }
            else {
                KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", "false");
                KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
            }
        }

        private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF = new AppConfigurationEntry[] {
                new AppConfigurationEntry(
                        KerberosUtil.getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        KEYTAB_KERBEROS_OPTIONS) };

        private javax.security.auth.login.Configuration baseConfig;
        private final String loginContextName;

        JaasConfiguration(String loginContextName, String principal, String keytabFile)
                throws IOException
        {
            this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
        }

        private JaasConfiguration(String loginContextName, String principal, String keytabFile, boolean useTicketCache)
                throws IOException
        {
            try {
                this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
            }
            catch (SecurityException e) {
                this.baseConfig = null;
            }
            this.loginContextName = loginContextName;

            if (!useTicketCache) {
                if (IS_IBM_JDK) {
                    KEYTAB_KERBEROS_OPTIONS.put("useKeytab", keytabFile);
                }
                else {
                    KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                }
            }
            KEYTAB_KERBEROS_OPTIONS.put("principal", principal);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "JaasConfiguration loginContextName="
                                + loginContextName
                                + " principal="
                                + principal
                                + " useTicketCache="
                                + useTicketCache
                                + " keytabFile="
                                + keytabFile);
            }
        }

        /**
         * getAppConfigurationEntry
         *
         * @param appName appName
         * @return AppConfigurationEntry
         */
        public AppConfigurationEntry[] getAppConfigurationEntry(String appName)
        {
            if (loginContextName.equals(appName)) {
                return KEYTAB_KERBEROS_CONF;
            }
            if (baseConfig != null) {
                return baseConfig.getAppConfigurationEntry(appName);
            }
            return new AppConfigurationEntry[0];
        }
    }
}
