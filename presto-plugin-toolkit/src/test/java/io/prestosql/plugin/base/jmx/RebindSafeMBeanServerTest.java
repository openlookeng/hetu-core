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
package io.prestosql.plugin.base.jmx;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;
import javax.management.loading.ClassLoaderRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class RebindSafeMBeanServerTest
{
    @Mock
    private MBeanServer mockMbeanServer;

    private RebindSafeMBeanServer rebindSafeMBeanServerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        rebindSafeMBeanServerUnderTest = new RebindSafeMBeanServer(mockMbeanServer);
    }

    @Test
    public void testRegisterMBean() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.registerMBean(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.registerMBean(any(Object.class), eq(new ObjectName("domain", "key", "value"))))
                .thenReturn(objectInstance);

        // Configure MBeanServer.getObjectInstance(...).
        final ObjectInstance objectInstance1 = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.getObjectInstance(new ObjectName("domain", "key", "value"))).thenReturn(objectInstance1);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.registerMBean("object", name);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRegisterMBean_MBeanServerRegisterMBeanThrowsInstanceAlreadyExistsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.registerMBean(any(Object.class), eq(new ObjectName("domain", "key", "value"))))
                .thenThrow(InstanceAlreadyExistsException.class);

        // Run the test
        assertThrows(InstanceAlreadyExistsException.class,
                () -> rebindSafeMBeanServerUnderTest.registerMBean("object", name));
    }

    @Test
    public void testRegisterMBean_MBeanServerRegisterMBeanThrowsMBeanRegistrationException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.registerMBean(any(Object.class), eq(new ObjectName("domain", "key", "value"))))
                .thenThrow(MBeanRegistrationException.class);

        // Run the test
        assertThrows(
                MBeanRegistrationException.class, () -> rebindSafeMBeanServerUnderTest.registerMBean("object", name));
    }

    @Test
    public void testRegisterMBean_MBeanServerRegisterMBeanThrowsNotCompliantMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.registerMBean(any(Object.class), eq(new ObjectName("domain", "key", "value"))))
                .thenThrow(NotCompliantMBeanException.class);

        // Run the test
        assertThrows(
                NotCompliantMBeanException.class, () -> rebindSafeMBeanServerUnderTest.registerMBean("object", name));
    }

    @Test
    public void testRegisterMBean_MBeanServerGetObjectInstanceThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.registerMBean(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.registerMBean(any(Object.class), eq(new ObjectName("domain", "key", "value"))))
                .thenReturn(objectInstance);

        when(mockMbeanServer.getObjectInstance(new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.registerMBean("object", name);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUnregisterMBean() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");

        // Run the test
        rebindSafeMBeanServerUnderTest.unregisterMBean(name);

        // Verify the results
        verify(mockMbeanServer).unregisterMBean(new ObjectName("domain", "key", "value"));
    }

    @Test
    public void testUnregisterMBean_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).unregisterMBean(
                new ObjectName("domain", "key", "value"));

        // Run the test
        assertThrows(InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.unregisterMBean(name));
    }

    @Test
    public void testUnregisterMBean_MBeanServerThrowsMBeanRegistrationException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        doThrow(MBeanRegistrationException.class).when(mockMbeanServer).unregisterMBean(
                new ObjectName("domain", "key", "value"));

        // Run the test
        assertThrows(MBeanRegistrationException.class, () -> rebindSafeMBeanServerUnderTest.unregisterMBean(name));
    }

    @Test
    public void testGetObjectInstance() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.getObjectInstance(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.getObjectInstance(new ObjectName("domain", "key", "value"))).thenReturn(objectInstance);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.getObjectInstance(name);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetObjectInstance_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getObjectInstance(new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.getObjectInstance(name));
    }

    @Test
    public void testQueryMBeans() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final QueryExp query = null;
        final Set<ObjectInstance> expectedResult = new HashSet<>(
                Arrays.asList(new ObjectInstance("objectName", "className")));

        // Configure MBeanServer.queryMBeans(...).
        final Set<ObjectInstance> objectInstances = new HashSet<>(
                Arrays.asList(new ObjectInstance("objectName", "className")));
        when(mockMbeanServer.queryMBeans(eq(new ObjectName("domain", "key", "value")), any(QueryExp.class)))
                .thenReturn(objectInstances);

        // Run the test
        final Set<ObjectInstance> result = rebindSafeMBeanServerUnderTest.queryMBeans(name, query);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testQueryMBeans_MBeanServerReturnsNoItems() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final QueryExp query = null;
        when(mockMbeanServer.queryMBeans(eq(new ObjectName("domain", "key", "value")), any(QueryExp.class)))
                .thenReturn(Collections.emptySet());

        // Run the test
        final Set<ObjectInstance> result = rebindSafeMBeanServerUnderTest.queryMBeans(name, query);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testQueryMBeans_MBeanServerThrowsRuntimeOperationsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final QueryExp query = null;
        when(mockMbeanServer.queryMBeans(eq(new ObjectName("domain", "key", "value")), any(QueryExp.class)))
                .thenThrow(RuntimeOperationsException.class);

        // Run the test
        assertThrows(RuntimeOperationsException.class, () -> rebindSafeMBeanServerUnderTest.queryMBeans(name, query));
    }

    @Test
    public void testQueryNames() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final QueryExp query = null;
        final Set<ObjectName> expectedResult = new HashSet<>(Arrays.asList(new ObjectName("domain", "key", "value")));

        // Configure MBeanServer.queryNames(...).
        final Set<ObjectName> objectNames = new HashSet<>(Arrays.asList(new ObjectName("domain", "key", "value")));
        when(mockMbeanServer.queryNames(eq(new ObjectName("domain", "key", "value")), any(QueryExp.class)))
                .thenReturn(objectNames);

        // Run the test
        final Set<ObjectName> result = rebindSafeMBeanServerUnderTest.queryNames(name, query);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testQueryNames_MBeanServerReturnsNoItems() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final QueryExp query = null;
        when(mockMbeanServer.queryNames(eq(new ObjectName("domain", "key", "value")), any(QueryExp.class)))
                .thenReturn(Collections.emptySet());

        // Run the test
        final Set<ObjectName> result = rebindSafeMBeanServerUnderTest.queryNames(name, query);

        // Verify the results
        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void testQueryNames_MBeanServerThrowsRuntimeOperationsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final QueryExp query = null;
        when(mockMbeanServer.queryNames(eq(new ObjectName("domain", "key", "value")), any(QueryExp.class)))
                .thenThrow(RuntimeOperationsException.class);

        // Run the test
        assertThrows(RuntimeOperationsException.class, () -> rebindSafeMBeanServerUnderTest.queryNames(name, query));
    }

    @Test
    public void testIsRegistered() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.isRegistered(new ObjectName("domain", "key", "value"))).thenReturn(false);

        // Run the test
        final boolean result = rebindSafeMBeanServerUnderTest.isRegistered(name);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsRegistered_MBeanServerThrowsRuntimeOperationsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.isRegistered(new ObjectName("domain", "key", "value")))
                .thenThrow(RuntimeOperationsException.class);

        // Run the test
        assertThrows(RuntimeOperationsException.class, () -> rebindSafeMBeanServerUnderTest.isRegistered(name));
    }

    @Test
    public void testGetMBeanCount()
    {
        // Setup
        when(mockMbeanServer.getMBeanCount()).thenReturn(0);

        // Run the test
        final Integer result = rebindSafeMBeanServerUnderTest.getMBeanCount();
    }

    @Test
    public void testGetAttribute() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttribute(new ObjectName("domain", "key", "value"), "attribute")).thenReturn("result");

        // Run the test
        final Object result = rebindSafeMBeanServerUnderTest.getAttribute(name, "attribute");

        // Verify the results
    }

    @Test
    public void testGetAttribute_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttribute(new ObjectName("domain", "key", "value"), "attribute"))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class, () -> rebindSafeMBeanServerUnderTest.getAttribute(name, "attribute"));
    }

    @Test
    public void testGetAttribute_MBeanServerThrowsAttributeNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttribute(new ObjectName("domain", "key", "value"), "attribute"))
                .thenThrow(AttributeNotFoundException.class);

        // Run the test
        assertThrows(
                AttributeNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.getAttribute(name, "attribute"));
    }

    @Test
    public void testGetAttribute_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttribute(new ObjectName("domain", "key", "value"), "attribute"))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.getAttribute(name, "attribute"));
    }

    @Test
    public void testGetAttribute_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttribute(new ObjectName("domain", "key", "value"), "attribute"))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.getAttribute(name, "attribute"));
    }

    @Test
    public void testGetAttributes() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final AttributeList expectedResult = new AttributeList(0);
        when(mockMbeanServer.getAttributes(eq(new ObjectName("domain", "key", "value")),
                any(String[].class))).thenReturn(new AttributeList(0));

        // Run the test
        final AttributeList result = rebindSafeMBeanServerUnderTest.getAttributes(name, new String[]{"attributes"});

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetAttributes_MBeanServerReturnsNoItems() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttributes(eq(new ObjectName("domain", "key", "value")),
                any(String[].class))).thenReturn(new AttributeList());

        // Run the test
        final AttributeList result = rebindSafeMBeanServerUnderTest.getAttributes(name, new String[]{"attributes"});

        // Verify the results
        assertEquals(new AttributeList(), result);
    }

    @Test
    public void testGetAttributes_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttributes(eq(new ObjectName("domain", "key", "value")),
                any(String[].class)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.getAttributes(name, new String[]{"attributes"}));
    }

    @Test
    public void testGetAttributes_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getAttributes(eq(new ObjectName("domain", "key", "value")),
                any(String[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.getAttributes(name, new String[]{"attributes"}));
    }

    @Test
    public void testSetAttribute() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final Attribute attribute = new Attribute("name", "value");

        // Run the test
        rebindSafeMBeanServerUnderTest.setAttribute(name, attribute);

        // Verify the results
        verify(mockMbeanServer).setAttribute(new ObjectName("domain", "key", "value"), new Attribute("name", "value"));
    }

    @Test
    public void testSetAttribute_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final Attribute attribute = new Attribute("name", "value");
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).setAttribute(
                new ObjectName("domain", "key", "value"), new Attribute("name", "value"));

        // Run the test
        assertThrows(
                InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.setAttribute(name, attribute));
    }

    @Test
    public void testSetAttribute_MBeanServerThrowsAttributeNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final Attribute attribute = new Attribute("name", "value");
        doThrow(AttributeNotFoundException.class).when(mockMbeanServer).setAttribute(
                new ObjectName("domain", "key", "value"), new Attribute("name", "value"));

        // Run the test
        assertThrows(
                AttributeNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.setAttribute(name, attribute));
    }

    @Test
    public void testSetAttribute_MBeanServerThrowsInvalidAttributeValueException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final Attribute attribute = new Attribute("name", "value");
        doThrow(InvalidAttributeValueException.class).when(mockMbeanServer).setAttribute(
                new ObjectName("domain", "key", "value"), new Attribute("name", "value"));

        // Run the test
        assertThrows(InvalidAttributeValueException.class,
                () -> rebindSafeMBeanServerUnderTest.setAttribute(name, attribute));
    }

    @Test
    public void testSetAttribute_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final Attribute attribute = new Attribute("name", "value");
        doThrow(MBeanException.class).when(mockMbeanServer).setAttribute(new ObjectName("domain", "key", "value"),
                new Attribute("name", "value"));

        // Run the test
        assertThrows(MBeanException.class, () -> rebindSafeMBeanServerUnderTest.setAttribute(name, attribute));
    }

    @Test
    public void testSetAttribute_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final Attribute attribute = new Attribute("name", "value");
        doThrow(ReflectionException.class).when(mockMbeanServer).setAttribute(new ObjectName("domain", "key", "value"),
                new Attribute("name", "value"));

        // Run the test
        assertThrows(ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.setAttribute(name, attribute));
    }

    @Test
    public void testSetAttributes() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final AttributeList attributes = new AttributeList(0);
        final AttributeList expectedResult = new AttributeList(0);
        when(mockMbeanServer.setAttributes(new ObjectName("domain", "key", "value"), new AttributeList(0)))
                .thenReturn(new AttributeList(0));

        // Run the test
        final AttributeList result = rebindSafeMBeanServerUnderTest.setAttributes(name, attributes);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSetAttributes_MBeanServerReturnsNoItems() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final AttributeList attributes = new AttributeList(0);
        when(mockMbeanServer.setAttributes(new ObjectName("domain", "key", "value"), new AttributeList(0)))
                .thenReturn(new AttributeList());

        // Run the test
        final AttributeList result = rebindSafeMBeanServerUnderTest.setAttributes(name, attributes);

        // Verify the results
        assertEquals(new AttributeList(), result);
    }

    @Test
    public void testSetAttributes_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final AttributeList attributes = new AttributeList(0);
        when(mockMbeanServer.setAttributes(new ObjectName("domain", "key", "value"), new AttributeList(0)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.setAttributes(name, attributes));
    }

    @Test
    public void testSetAttributes_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final AttributeList attributes = new AttributeList(0);
        when(mockMbeanServer.setAttributes(new ObjectName("domain", "key", "value"), new AttributeList(0)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.setAttributes(name, attributes));
    }

    @Test
    public void testInvoke() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.invoke(eq(new ObjectName("domain", "key", "value")), eq("operationName"),
                any(Object[].class), any(String[].class))).thenReturn("result");

        // Run the test
        final Object result = rebindSafeMBeanServerUnderTest.invoke(name, "operationName", new Object[]{"params"},
                new String[]{"signature"});

        // Verify the results
    }

    @Test
    public void testInvoke_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.invoke(eq(new ObjectName("domain", "key", "value")), eq("operationName"),
                any(Object[].class), any(String[].class)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.invoke(name, "operationName", new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testInvoke_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.invoke(eq(new ObjectName("domain", "key", "value")), eq("operationName"),
                any(Object[].class), any(String[].class)))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.invoke(name, "operationName", new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testInvoke_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.invoke(eq(new ObjectName("domain", "key", "value")), eq("operationName"),
                any(Object[].class), any(String[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.invoke(name, "operationName", new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testGetDefaultDomain()
    {
        // Setup
        when(mockMbeanServer.getDefaultDomain()).thenReturn("result");

        // Run the test
        final String result = rebindSafeMBeanServerUnderTest.getDefaultDomain();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetDomains()
    {
        // Setup
        when(mockMbeanServer.getDomains()).thenReturn(new String[]{"result"});

        // Run the test
        final String[] result = rebindSafeMBeanServerUnderTest.getDomains();

        // Verify the results
        assertEquals(new String[]{"result"}, result);
    }

    @Test
    public void testGetDomains_MBeanServerReturnsNoItems()
    {
        // Setup
        when(mockMbeanServer.getDomains()).thenReturn(new String[]{});

        // Run the test
        final String[] result = rebindSafeMBeanServerUnderTest.getDomains();

        // Verify the results
        assertEquals(new String[]{}, result);
    }

    @Test
    public void testAddNotificationListener1() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        final NotificationFilter filter = null;

        // Run the test
        rebindSafeMBeanServerUnderTest.addNotificationListener(name, mockListener, filter, "context");

        // Verify the results
        verify(mockMbeanServer).addNotificationListener(eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class), any(NotificationFilter.class), any(Object.class));
    }

    @Test
    public void testAddNotificationListener1_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        final NotificationFilter filter = null;
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).addNotificationListener(
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class), any(NotificationFilter.class), any(Object.class));

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.addNotificationListener(name, mockListener, filter, "context"));
    }

    @Test
    public void testAddNotificationListener2() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        final NotificationFilter filter = null;

        // Run the test
        rebindSafeMBeanServerUnderTest.addNotificationListener(name, mockListener, filter, "context");

        // Verify the results
        verify(mockMbeanServer).addNotificationListener(eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationFilter.class), any(Object.class));
    }

    @Test
    public void testAddNotificationListener2_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        final NotificationFilter filter = null;
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).addNotificationListener(
                eq(new ObjectName("domain", "key", "value")), eq(new ObjectName("domain", "key", "value")),
                any(NotificationFilter.class), any(Object.class));

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.addNotificationListener(name, mockListener, filter, "context"));
    }

    @Test
    public void testRemoveNotificationListener1() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);

        // Run the test
        rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener);

        // Verify the results
        verify(mockMbeanServer).removeNotificationListener(new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value"));
    }

    @Test
    public void testRemoveNotificationListener1_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                new ObjectName("domain", "key", "value"), new ObjectName("domain", "key", "value"));

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener));
    }

    @Test
    public void testRemoveNotificationListener1_MBeanServerThrowsListenerNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        doThrow(ListenerNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                new ObjectName("domain", "key", "value"), new ObjectName("domain", "key", "value"));

        // Run the test
        assertThrows(ListenerNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener));
    }

    @Test
    public void testRemoveNotificationListener2() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        final NotificationFilter filter = null;

        // Run the test
        rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener, filter, "context");

        // Verify the results
        verify(mockMbeanServer).removeNotificationListener(eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationFilter.class), any(Object.class));
    }

    @Test
    public void testRemoveNotificationListener2_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        final NotificationFilter filter = null;
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                eq(new ObjectName("domain", "key", "value")), eq(new ObjectName("domain", "key", "value")),
                any(NotificationFilter.class), any(Object.class));

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener, filter, "context"));
    }

    @Test
    public void testRemoveNotificationListener2_MBeanServerThrowsListenerNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName mockListener = mock(ObjectName.class);
        final NotificationFilter filter = null;
        doThrow(ListenerNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                eq(new ObjectName("domain", "key", "value")), eq(new ObjectName("domain", "key", "value")),
                any(NotificationFilter.class), any(Object.class));

        // Run the test
        assertThrows(ListenerNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener, filter, "context"));
    }

    @Test
    public void testRemoveNotificationListener3() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);

        // Run the test
        rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener);

        // Verify the results
        verify(mockMbeanServer).removeNotificationListener(eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class));
    }

    @Test
    public void testRemoveNotificationListener3_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class));

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener));
    }

    @Test
    public void testRemoveNotificationListener3_MBeanServerThrowsListenerNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        doThrow(ListenerNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class));

        // Run the test
        assertThrows(ListenerNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener));
    }

    @Test
    public void testRemoveNotificationListener4() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        final NotificationFilter filter = null;

        // Run the test
        rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener, filter, "context");

        // Verify the results
        verify(mockMbeanServer).removeNotificationListener(eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class), any(NotificationFilter.class), any(Object.class));
    }

    @Test
    public void testRemoveNotificationListener4_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        final NotificationFilter filter = null;
        doThrow(InstanceNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class), any(NotificationFilter.class), any(Object.class));

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener, filter, "context"));
    }

    @Test
    public void testRemoveNotificationListener4_MBeanServerThrowsListenerNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final NotificationListener mockListener = mock(NotificationListener.class);
        final NotificationFilter filter = null;
        doThrow(ListenerNotFoundException.class).when(mockMbeanServer).removeNotificationListener(
                eq(new ObjectName("domain", "key", "value")),
                any(NotificationListener.class), any(NotificationFilter.class), any(Object.class));

        // Run the test
        assertThrows(ListenerNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.removeNotificationListener(name, mockListener, filter, "context"));
    }

    @Test
    public void testGetMBeanInfo() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final MBeanInfo expectedResult = new MBeanInfo("className", "description",
                new MBeanAttributeInfo[]{new MBeanAttributeInfo("name", "type", "description", false, false, false)},
                new MBeanConstructorInfo[]{new MBeanConstructorInfo("name", "description",
                        new MBeanParameterInfo[]{new MBeanParameterInfo("name", "type", "description")})},
                new MBeanOperationInfo[]{new MBeanOperationInfo("name", "description",
                        new MBeanParameterInfo[]{new MBeanParameterInfo("name", "type", "description")}, "type", 0)},
                new MBeanNotificationInfo[]{new MBeanNotificationInfo(new String[]{"notifTypes"}, "name",
                        "description")});

        // Configure MBeanServer.getMBeanInfo(...).
        final MBeanInfo mBeanInfo = new MBeanInfo("className", "description",
                new MBeanAttributeInfo[]{new MBeanAttributeInfo("name", "type", "description", false, false, false)},
                new MBeanConstructorInfo[]{new MBeanConstructorInfo("name", "description",
                        new MBeanParameterInfo[]{new MBeanParameterInfo("name", "type", "description")})},
                new MBeanOperationInfo[]{new MBeanOperationInfo("name", "description",
                        new MBeanParameterInfo[]{new MBeanParameterInfo("name", "type", "description")}, "type", 0)},
                new MBeanNotificationInfo[]{new MBeanNotificationInfo(new String[]{"notifTypes"}, "name",
                        "description")});
        when(mockMbeanServer.getMBeanInfo(new ObjectName("domain", "key", "value"))).thenReturn(mBeanInfo);

        // Run the test
        final MBeanInfo result = rebindSafeMBeanServerUnderTest.getMBeanInfo(name);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetMBeanInfo_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getMBeanInfo(new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.getMBeanInfo(name));
    }

    @Test
    public void testGetMBeanInfo_MBeanServerThrowsIntrospectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getMBeanInfo(new ObjectName("domain", "key", "value")))
                .thenThrow(IntrospectionException.class);

        // Run the test
        assertThrows(IntrospectionException.class, () -> rebindSafeMBeanServerUnderTest.getMBeanInfo(name));
    }

    @Test
    public void testGetMBeanInfo_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getMBeanInfo(new ObjectName("domain", "key", "value")))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.getMBeanInfo(name));
    }

    @Test
    public void testIsInstanceOf() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.isInstanceOf(new ObjectName("domain", "key", "value"), "className")).thenReturn(false);

        // Run the test
        final boolean result = rebindSafeMBeanServerUnderTest.isInstanceOf(name, "className");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsInstanceOf_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.isInstanceOf(new ObjectName("domain", "key", "value"), "className"))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.isInstanceOf(name, "className"));
    }

    @Test
    public void testInstantiate1() throws Exception
    {
        // Setup
        when(mockMbeanServer.instantiate("className")).thenReturn("result");

        // Run the test
        final Object result = rebindSafeMBeanServerUnderTest.instantiate("className");

        // Verify the results
    }

    @Test
    public void testInstantiate1_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        when(mockMbeanServer.instantiate("className")).thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.instantiate("className"));
    }

    @Test
    public void testInstantiate1_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        when(mockMbeanServer.instantiate("className")).thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class, () -> rebindSafeMBeanServerUnderTest.instantiate("className"));
    }

    @Test
    public void testInstantiate2() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate("className", new ObjectName("domain", "key", "value"))).thenReturn("result");

        // Run the test
        final Object result = rebindSafeMBeanServerUnderTest.instantiate("className", loaderName);

        // Verify the results
    }

    @Test
    public void testInstantiate2_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate("className", new ObjectName("domain", "key", "value")))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(
                ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.instantiate("className", loaderName));
    }

    @Test
    public void testInstantiate2_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate("className", new ObjectName("domain", "key", "value")))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class, () -> rebindSafeMBeanServerUnderTest.instantiate("className", loaderName));
    }

    @Test
    public void testInstantiate2_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate("className", new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.instantiate("className", loaderName));
    }

    @Test
    public void testInstantiate3() throws Exception
    {
        // Setup
        when(mockMbeanServer.instantiate(eq("className"), any(Object[].class), any(String[].class)))
                .thenReturn("result");

        // Run the test
        final Object result = rebindSafeMBeanServerUnderTest.instantiate("className", new Object[]{"params"},
                new String[]{"signature"});

        // Verify the results
    }

    @Test
    public void testInstantiate3_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        when(mockMbeanServer.instantiate(eq("className"), any(Object[].class), any(String[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.instantiate("className", new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testInstantiate3_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        when(mockMbeanServer.instantiate(eq("className"), any(Object[].class), any(String[].class)))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.instantiate("className", new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testInstantiate4() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class))).thenReturn("result");

        // Run the test
        final Object result = rebindSafeMBeanServerUnderTest.instantiate("className", loaderName,
                new Object[]{"params"}, new String[]{"signature"});

        // Verify the results
    }

    @Test
    public void testInstantiate4_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(
                ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.instantiate("className", loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testInstantiate4_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(
                MBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.instantiate("className", loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testInstantiate4_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.instantiate(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.instantiate("className", loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testDeserialize1() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");

        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(
                new ObjectInputStream(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8))));
        when(mockMbeanServer.deserialize(eq(new ObjectName("domain", "key", "value")), any(byte[].class)))
                .thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize(name, "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize1_MBeanServerReturnsNoContent() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");

        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(
                new ObjectInputStream(new ByteArrayInputStream(new byte[]{})));
        when(mockMbeanServer.deserialize(eq(new ObjectName("domain", "key", "value")), any(byte[].class)))
                .thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize(name, "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize1_MBeanServerReturnsBrokenIo() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");

        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(new ObjectInputStream(new InputStream() {
            private final IOException exception = new IOException("Error");

            @Override
            public int read() throws IOException
            {
                throw exception;
            }

            @Override
            public int available() throws IOException
            {
                throw exception;
            }

            @Override
            public long skip(final long n) throws IOException
            {
                throw exception;
            }

            @Override
            public synchronized void reset() throws IOException
            {
                throw exception;
            }

            @Override
            public void close() throws IOException
            {
                throw exception;
            }
        }));
        when(mockMbeanServer.deserialize(eq(new ObjectName("domain", "key", "value")), any(byte[].class)))
                .thenReturn(spyObjectInputStream);

        // Run the test
        assertThrows(OperationsException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize(name, "content".getBytes(StandardCharsets.UTF_8)));
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize1_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.deserialize(eq(new ObjectName("domain", "key", "value")), any(byte[].class)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize(name, "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDeserialize1_MBeanServerThrowsOperationsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.deserialize(eq(new ObjectName("domain", "key", "value")), any(byte[].class)))
                .thenThrow(OperationsException.class);

        // Run the test
        assertThrows(OperationsException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize(name, "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDeserialize2() throws Exception
    {
        // Setup
        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(
                new ObjectInputStream(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8))));
        when(mockMbeanServer.deserialize(eq("className"), any(byte[].class))).thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize("className", "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize2_MBeanServerReturnsNoContent() throws Exception
    {
        // Setup
        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(
                new ObjectInputStream(new ByteArrayInputStream(new byte[]{})));
        when(mockMbeanServer.deserialize(eq("className"), any(byte[].class))).thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize("className", "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize2_MBeanServerReturnsBrokenIo() throws Exception
    {
        // Setup
        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(new ObjectInputStream(new InputStream() {
            private final IOException exception = new IOException("Error");

            @Override
            public int read() throws IOException
            {
                throw exception;
            }

            @Override
            public int available() throws IOException
            {
                throw exception;
            }

            @Override
            public long skip(final long n) throws IOException
            {
                throw exception;
            }

            @Override
            public synchronized void reset() throws IOException
            {
                throw exception;
            }

            @Override
            public void close() throws IOException
            {
                throw exception;
            }
        }));
        when(mockMbeanServer.deserialize(eq("className"), any(byte[].class))).thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize("className", "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize2_MBeanServerThrowsOperationsException() throws Exception
    {
        // Setup
        when(mockMbeanServer.deserialize(eq("className"), any(byte[].class))).thenThrow(OperationsException.class);

        // Run the test
        assertThrows(
                OperationsException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize("className", "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDeserialize2_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        when(mockMbeanServer.deserialize(eq("className"), any(byte[].class))).thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(
                ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize("className", "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDeserialize3() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");

        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(
                new ObjectInputStream(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8))));
        when(mockMbeanServer.deserialize(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(byte[].class))).thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize("className", loaderName,
                "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize3_MBeanServerReturnsNoContent() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");

        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(
                new ObjectInputStream(new ByteArrayInputStream(new byte[]{})));
        when(mockMbeanServer.deserialize(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(byte[].class))).thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize("className", loaderName,
                "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize3_MBeanServerReturnsBrokenIo() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");

        // Configure MBeanServer.deserialize(...).
        final ObjectInputStream spyObjectInputStream = spy(new ObjectInputStream(new InputStream() {
            private final IOException exception = new IOException("Error");

            @Override
            public int read() throws IOException
            {
                throw exception;
            }

            @Override
            public int available() throws IOException
            {
                throw exception;
            }

            @Override
            public long skip(final long n) throws IOException
            {
                throw exception;
            }

            @Override
            public synchronized void reset() throws IOException
            {
                throw exception;
            }

            @Override
            public void close() throws IOException
            {
                throw exception;
            }
        }));
        when(mockMbeanServer.deserialize(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(byte[].class))).thenReturn(spyObjectInputStream);

        // Run the test
        final ObjectInputStream result = rebindSafeMBeanServerUnderTest.deserialize("className", loaderName,
                "content".getBytes(StandardCharsets.UTF_8));

        // Verify the results
        verify(spyObjectInputStream).close();
    }

    @Test
    public void testDeserialize3_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.deserialize(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(byte[].class)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize("className", loaderName, "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDeserialize3_MBeanServerThrowsOperationsException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.deserialize(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(byte[].class)))
                .thenThrow(OperationsException.class);

        // Run the test
        assertThrows(
                OperationsException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize("className", loaderName, "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testDeserialize3_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.deserialize(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(byte[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(
                ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.deserialize("className", loaderName, "content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testGetClassLoaderFor() throws Exception
    {
        // Setup
        final ObjectName mbeanName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getClassLoaderFor(new ObjectName("domain", "key", "value")))
                .thenReturn(ClassLoader.getSystemClassLoader());

        // Run the test
        final ClassLoader result = rebindSafeMBeanServerUnderTest.getClassLoaderFor(mbeanName);

        // Verify the results
    }

    @Test
    public void testGetClassLoaderFor_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName mbeanName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getClassLoaderFor(new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.getClassLoaderFor(mbeanName));
    }

    @Test
    public void testGetClassLoader() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getClassLoader(new ObjectName("domain", "key", "value")))
                .thenReturn(ClassLoader.getSystemClassLoader());

        // Run the test
        final ClassLoader result = rebindSafeMBeanServerUnderTest.getClassLoader(loaderName);

        // Verify the results
    }

    @Test
    public void testGetClassLoader_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.getClassLoader(new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(InstanceNotFoundException.class, () -> rebindSafeMBeanServerUnderTest.getClassLoader(loaderName));
    }

    @Test
    public void testGetClassLoaderRepository()
    {
        // Setup
        when(mockMbeanServer.getClassLoaderRepository()).thenReturn(null);

        // Run the test
        final ClassLoaderRepository result = rebindSafeMBeanServerUnderTest.getClassLoaderRepository();

        // Verify the results
    }

    @Test
    public void testCreateMBean1() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.createMBean(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value")))
                .thenReturn(objectInstance);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.createMBean("className", name);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateMBean1_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value")))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class, () -> rebindSafeMBeanServerUnderTest.createMBean("className", name));
    }

    @Test
    public void testCreateMBean1_MBeanServerThrowsInstanceAlreadyExistsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceAlreadyExistsException.class);

        // Run the test
        assertThrows(InstanceAlreadyExistsException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name));
    }

    @Test
    public void testCreateMBean1_MBeanServerThrowsMBeanRegistrationException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value")))
                .thenThrow(MBeanRegistrationException.class);

        // Run the test
        assertThrows(
                MBeanRegistrationException.class, () -> rebindSafeMBeanServerUnderTest.createMBean("className", name));
    }

    @Test
    public void testCreateMBean1_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value")))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class, () -> rebindSafeMBeanServerUnderTest.createMBean("className", name));
    }

    @Test
    public void testCreateMBean1_MBeanServerThrowsNotCompliantMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value")))
                .thenThrow(NotCompliantMBeanException.class);

        // Run the test
        assertThrows(
                NotCompliantMBeanException.class, () -> rebindSafeMBeanServerUnderTest.createMBean("className", name));
    }

    @Test
    public void testCreateMBean2() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.createMBean(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value"))).thenReturn(objectInstance);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateMBean2_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value")))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName));
    }

    @Test
    public void testCreateMBean2_MBeanServerThrowsInstanceAlreadyExistsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceAlreadyExistsException.class);

        // Run the test
        assertThrows(InstanceAlreadyExistsException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName));
    }

    @Test
    public void testCreateMBean2_MBeanServerThrowsMBeanRegistrationException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value")))
                .thenThrow(MBeanRegistrationException.class);

        // Run the test
        assertThrows(
                MBeanRegistrationException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName));
    }

    @Test
    public void testCreateMBean2_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value")))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(
                MBeanException.class, () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName));
    }

    @Test
    public void testCreateMBean2_MBeanServerThrowsNotCompliantMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value")))
                .thenThrow(NotCompliantMBeanException.class);

        // Run the test
        assertThrows(
                NotCompliantMBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName));
    }

    @Test
    public void testCreateMBean2_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean("className", new ObjectName("domain", "key", "value"),
                new ObjectName("domain", "key", "value")))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName));
    }

    @Test
    public void testCreateMBean3() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.createMBean(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class))).thenReturn(objectInstance);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.createMBean("className", name,
                new Object[]{"params"}, new String[]{"signature"});

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateMBean3_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(
                ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean3_MBeanServerThrowsInstanceAlreadyExistsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(InstanceAlreadyExistsException.class);

        // Run the test
        assertThrows(InstanceAlreadyExistsException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean3_MBeanServerThrowsMBeanRegistrationException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(MBeanRegistrationException.class);

        // Run the test
        assertThrows(
                MBeanRegistrationException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean3_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(MBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean3_MBeanServerThrowsNotCompliantMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                any(Object[].class), any(String[].class)))
                .thenThrow(NotCompliantMBeanException.class);

        // Run the test
        assertThrows(
                NotCompliantMBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean4() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        final ObjectInstance expectedResult = new ObjectInstance("objectName", "className");

        // Configure MBeanServer.createMBean(...).
        final ObjectInstance objectInstance = new ObjectInstance("objectName", "className");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenReturn(objectInstance);

        // Run the test
        final ObjectInstance result = rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName,
                new Object[]{"params"}, new String[]{"signature"});

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCreateMBean4_MBeanServerThrowsReflectionException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenThrow(ReflectionException.class);

        // Run the test
        assertThrows(ReflectionException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean4_MBeanServerThrowsInstanceAlreadyExistsException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenThrow(InstanceAlreadyExistsException.class);

        // Run the test
        assertThrows(InstanceAlreadyExistsException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean4_MBeanServerThrowsMBeanRegistrationException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenThrow(MBeanRegistrationException.class);

        // Run the test
        assertThrows(
                MBeanRegistrationException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean4_MBeanServerThrowsMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenThrow(MBeanException.class);

        // Run the test
        assertThrows(
                MBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean4_MBeanServerThrowsNotCompliantMBeanException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenThrow(NotCompliantMBeanException.class);

        // Run the test
        assertThrows(
                NotCompliantMBeanException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }

    @Test
    public void testCreateMBean4_MBeanServerThrowsInstanceNotFoundException() throws Exception
    {
        // Setup
        final ObjectName name = new ObjectName("domain", "key", "value");
        final ObjectName loaderName = new ObjectName("domain", "key", "value");
        when(mockMbeanServer.createMBean(eq("className"), eq(new ObjectName("domain", "key", "value")),
                eq(new ObjectName("domain", "key", "value")), any(Object[].class), any(String[].class)))
                .thenThrow(InstanceNotFoundException.class);

        // Run the test
        assertThrows(
                InstanceNotFoundException.class,
                () -> rebindSafeMBeanServerUnderTest.createMBean("className", name, loaderName, new Object[]{"params"},
                        new String[]{"signature"}));
    }
}
