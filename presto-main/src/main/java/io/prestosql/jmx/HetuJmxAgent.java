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
package io.prestosql.jmx;

import io.airlift.log.Logger;
import io.hetu.core.common.util.SslSocketUtil;

import javax.inject.Inject;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ServerSocketFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;

public class HetuJmxAgent
{
    private static final Logger LOG = Logger.get(HetuJmxAgent.class);
    private static JMXConnectorServer jmxConnectorServer;
    private static final String RMI_REGISTRY_HOST_DEFAULT = "localhost";

    private JMXServiceURL url;

    @Inject
    public HetuJmxAgent(HetuJmxConfig config)
            throws IOException
    {
        String jmxServerHostname;
        if (config.getRmiServerHostname() == null) {
            jmxServerHostname = RMI_REGISTRY_HOST_DEFAULT;
        }
        else {
            jmxServerHostname = config.getRmiServerHostname();
        }

        int jmxRegistryPort;
        if (config.getRmiRegistryPort() == null) {
            jmxRegistryPort = SslSocketUtil.getAvailablePort();
        }
        else {
            jmxRegistryPort = config.getRmiRegistryPort();
        }

        int jmxServerPort = jmxRegistryPort;
        if (config.getRmiServerPort() != null) {
            jmxServerPort = config.getRmiServerPort();
        }

        synchronized (HetuJmxAgent.class) {
            if (jmxConnectorServer != null) {
                LOG.info("JMX Server has already been started at Registry port %s", jmxRegistryPort);
            }
            else {
                startJmxServer(jmxServerHostname, jmxServerPort, jmxRegistryPort);
                LOG.info("JMX Server started at Registry port %s", jmxRegistryPort);
            }
        }
    }

    private void startJmxServer(String jmxServerHostname, int jmxServerPort, int jmxRegistryPort)
            throws IOException
    {
        // Environment map
        HashMap<String, Object> jmxEnv = new HashMap<>();
        jmxEnv.put(RMIConnectorServer.JNDI_REBIND_ATTRIBUTE, "TRUE");

        RMIServerSocketFactory serverFactory = new RMIServerSocketFactoryImpl(InetAddress.getByName(jmxServerHostname));
        jmxEnv.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);

        // Create the RMI registry
        LocateRegistry.createRegistry(jmxRegistryPort, null, serverFactory);

        // Retrieve the PlatformMBeanServer.
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Build jmxURL
        url = buildJmxUrl(jmxServerHostname, jmxRegistryPort, jmxServerPort);

        // Start the JMXListener with the connection string
        jmxConnectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, jmxEnv, mBeanServer);
        jmxConnectorServer.start();
    }

    public JMXServiceURL getUrl()
    {
        return url;
    }

    private JMXServiceURL buildJmxUrl(String rmiHostname, int jmxRegistryPort, int jmxConnectorServerPort)
            throws MalformedURLException
    {
        StringBuilder builder = new StringBuilder();
        builder.append("service:jmx:rmi://").append(rmiHostname).append(":").append(jmxConnectorServerPort);
        builder.append("/jndi/rmi://").append(rmiHostname).append(":").append(jmxRegistryPort).append("/jmxrmi");

        LOG.info("Building jmx url.");
        return new JMXServiceURL(builder.toString());
    }

    private class RMIServerSocketFactoryImpl
            implements RMIServerSocketFactory
    {
        private final InetAddress localAddress;

        public RMIServerSocketFactoryImpl(final InetAddress pAddress)
        {
            localAddress = pAddress;
        }

        @Override
        public ServerSocket createServerSocket(final int pPort) throws IOException
        {
            return ServerSocketFactory.getDefault() .createServerSocket(pPort, 0, localAddress);
        }
    }
}
