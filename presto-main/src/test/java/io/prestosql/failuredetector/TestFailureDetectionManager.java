/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.failuredetector;

import io.prestosql.spi.failuredetector.FailureRetryPolicy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestFailureDetectionManager
{
    private FailureRetryConfig cfg;
    private FailureRetryConfig cfg1;
    private FailureDetectorManager failureDetectorManager;
    private FailureDetectorManager failureDetectorManager1;
    private FailureDetectorManager failureDetectorManager2;

    @BeforeClass
    public void setUp()
    {
        cfg = new FailureRetryConfig();
        cfg.setFailureRetryPolicyProfile("test2");
        Properties prop = new Properties();
        prop.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, FailureRetryPolicy.MAXRETRY);
        prop.setProperty(FailureRetryPolicy.MAX_RETRY_COUNT, "10");
        prop.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, "60s");
        FailureDetectorManager.addFrConfigs(cfg.getFailureRetryPolicyProfile(), prop);
        FailureDetectorManager.addFailureRetryFactory(new MaxRetryFailureRetryFactory());

        failureDetectorManager = new FailureDetectorManager(cfg, new NoOpFailureDetector());

        cfg1 = new FailureRetryConfig();
        cfg1.setFailureRetryPolicyProfile("default1");
        Properties prop1 = new Properties();
        prop1.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, FailureRetryPolicy.MAXRETRY);
        prop1.setProperty(FailureRetryPolicy.MAX_RETRY_COUNT, "100");
        prop1.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, "34560s");
        FailureDetectorManager.addFrConfigs(cfg1.getFailureRetryPolicyProfile(), prop1);

        failureDetectorManager1 = new FailureDetectorManager(cfg1, new NoOpFailureDetector());

        failureDetectorManager2 = new FailureDetectorManager(new NoOpFailureDetector(), "30s");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        FailureDetectorManager.removeallFrConfigs();
    }

    @Test
    public void testProfileConfigsCount()
    {
        Map<String, Properties> frConfigs = FailureDetectorManager.getAvailableFrConfigs();
        assertNotNull(frConfigs.get("default1"));
        assertNotNull(frConfigs.get("test2"));
        assertNotNull(frConfigs.get("default"));
    }

    @Test
    public void testDefaultRetryProfile()
    {
        Map<String, Properties> cfgProp = FailureDetectorManager.getAvailableFrConfigs();
        Properties checkprop = cfgProp.get("test2");
        assertEquals(FailureRetryPolicy.MAXRETRY, checkprop.getProperty(FailureRetryPolicy.FD_RETRY_TYPE));
        assertEquals("10", checkprop.getProperty(FailureRetryPolicy.MAX_RETRY_COUNT));
        assertEquals("60s", checkprop.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION));
    }

    @Test
    public void testDefaultRetryProfile1()
    {
        Map<String, Properties> cfgProp = FailureDetectorManager.getAvailableFrConfigs();
        Properties checkprop = cfgProp.get("default1");
        assertEquals(FailureRetryPolicy.MAXRETRY, checkprop.getProperty(FailureRetryPolicy.FD_RETRY_TYPE));
        assertEquals("100", checkprop.getProperty(FailureRetryPolicy.MAX_RETRY_COUNT));
        assertEquals("34560s", checkprop.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION));
    }

    @Test
    public void testDefaultRetryProfile2()
    {
        Map<String, Properties> cfgProp = FailureDetectorManager.getAvailableFrConfigs();
        Properties checkprop = cfgProp.get(failureDetectorManager2.getFailureRetryPolicyUserProfile());
        assertEquals("default", failureDetectorManager2.getFailureRetryPolicyUserProfile());
        assertEquals(FailureRetryPolicy.TIMEOUT, checkprop.getProperty(FailureRetryPolicy.FD_RETRY_TYPE));
        assertEquals("30s", checkprop.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION));
    }
}
