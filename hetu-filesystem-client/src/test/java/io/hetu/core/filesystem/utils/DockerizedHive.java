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
package io.hetu.core.filesystem.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.testing.docker.DockerContainer;
import io.prestosql.testing.docker.DockerContainerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * <pre>
 * Class that manages a hive docker container.
 * The following requirements must be met in order to run this class:
 * 1. The current user must have permission to use Docker without sudo
 * 2. The Hive image must be loaded into Docker: docker pull prestodev/hdp2.6-hive:13
 * The test class will try to load this image from docker hub, but there may be proxy issues
 * 3. Need to make sure hadoop-master is mapped to 127.0.0.1 in /etc/hosts setting.
 * </pre>
 */
public class DockerizedHive
        implements Closeable
{
    private static final String DOCKER_HIVE_HOST_NAME = "hadoop-master";
    private static final String DOCKER_HIVE_NETWORK_NAME = "test_docker_hive";
    private static final String HDFS_AUTHENTICATION_KERBEROS = "KERBEROS";
    private static DockerizedHive instance;
    private static AtomicInteger instanceCount = new AtomicInteger(0);
    private ImmutableList<Integer> ports = ImmutableList.of(1180, 8020, 8042, 8088, 9000, 9083, 10000, 19888, 50070, 50075);
    private DockerContainer dockerContainer;

    private Configuration config;

    private TempFolder configFolder;

    private Properties properties;

    private String hdfsSitePath = TestConstantsHelper.DOCKER_HDFS_SITE_FILE;

    private String coreSitePath = TestConstantsHelper.DOCKER_CORE_SITE_FILE;

    private String hivePropertiesPath = TestConstantsHelper.DOCKER_HIVE_PROPERTIES_FILE;

    private DockerContainer.HostPortProvider hostPortProvider;

    private FileSystem fs;

    public static Configuration getContainerConfig(String testName)
    {
        try {
            DockerizedHive container = getInstance();
            return container.getHadoopConfiguration();
        }
        catch (IllegalStateException e) {
            // Docker not started. Skip test.
            System.out.println(String.format(
                    "## Docker service not started. Skip test: %s. " +
                            "Please refer to READMD.md for set up guide. ##",
                    testName));
            return null;
        }
        catch (Exception e) {
            // Failed to initialize docker environment. Skip test.
            System.out.println(String.format(
                    "## Docker environment not properly set up. Skip test: %s. " +
                            "Please refer to READMD.md for set up guide. ##",
                    testName));
            System.out.println("Error message:");
            e.printStackTrace();
            return null;
        }
    }

    // Note: need to add "127.0.0.1 hadoop-master" to "/etc/hosts"
    private DockerizedHive()
            throws IOException
    {
        checkFileExist(hdfsSitePath);
        checkFileExist(coreSitePath);
        checkFileExist(hivePropertiesPath);
        checkHostnameResolution(DOCKER_HIVE_HOST_NAME);

        properties = new Properties();
        try (InputStream is = new FileInputStream(hivePropertiesPath)) {
            properties.load(is);
        }

        // clean up hook in case the test case was interrupted by unknown source
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (dockerContainer != null) {
                dockerContainer.close();
            }
        }));

        // Note: the DockerContainer initialization won't be finished until the healthCheck returns without exception
        this.dockerContainer = new DockerContainer(new DockerContainerConfig(
                "prestodev/hdp2.6-hive:13",
                ports,
                Collections.emptyList(),
                Collections.emptyMap(),
                DOCKER_HIVE_HOST_NAME,
                DOCKER_HIVE_NETWORK_NAME),
                this::healthCheck);
    }

    public static synchronized DockerizedHive getInstance()
            throws IOException
    {
        instanceCount.getAndIncrement();
        if (instance == null) {
            instance = new DockerizedHive();
        }
        return instance;
    }

    private void healthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws IOException
    {
        // if the service is not up, this will throw an error
        this.hostPortProvider = hostPortProvider;
        FileSystem fs = getFs();
        fs.exists(new Path("/"));
    }

    private void checkHostnameResolution(String hostname)
    {
        try {
            InetAddress.getByName(hostname);
        }
        catch (UnknownHostException e) {
            throw new IllegalStateException(
                    String.format("Make sure '127.0.0.1 %s' is added to /etc/hosts", DOCKER_HIVE_HOST_NAME), e);
        }
    }

    private void checkFileExist(String path)
    {
        File file = new File(path);
        Preconditions.checkArgument(file.exists(), file.getAbsolutePath() + " is not found");
    }

    public synchronized Configuration getHadoopConfiguration()
            throws IOException
    {
        if (config == null) {
            // Generate config object from the site path
            correctPortMapping();
            config = new Configuration();
            config.addResource(new Path(hdfsSitePath));
            config.addResource(new Path(coreSitePath));

            if (HDFS_AUTHENTICATION_KERBEROS.equals(properties.getProperty("hive.hdfs.authentication.type"))) {
                String hdfsKeyTabPath = requireNonNull(properties.getProperty("hive.hdfs.hetu.keytab"));
                String hiveKrb5ConfPath = requireNonNull(properties.getProperty("hive.metastore.krb5.conf.path"));
                String hdfsPrincipal = requireNonNull(properties.getProperty("hive.hdfs.hetu.principal"));

                checkFileExist(hdfsKeyTabPath);
                checkFileExist(hiveKrb5ConfPath);
                checkFileExist(hdfsKeyTabPath);

                System.setProperty("java.security.krb5.conf", hiveKrb5ConfPath);
                UserGroupInformation.setConfiguration(config);
                UserGroupInformation.loginUserFromKeytab(hdfsPrincipal, hdfsKeyTabPath);
            }
        }
        return config;
    }

    public synchronized FileSystem getFs()
            throws IOException
    {
        if (fs == null) {
            Configuration hadoopConfig = getHadoopConfiguration();
            fs = FileSystem.get(hadoopConfig);
        }
        return fs;
    }

    /**
     * Adjust the Hadoop Configuration object and hive properties object by creating two temporary mock core-site.xml
     * and hdfs-site.xml that contains the correct port to the container.
     */
    private void correctPortMapping()
    {
        // We'll create two temporary core-site.xml and hdfs-site.xml that contains the correct port to the
        // running docker container. Then, we'll update the hive.properties to point to these two temp files.
        File coreSite;
        File hdfsSite;
        try {
            configFolder = new TempFolder().create();
            coreSite = configFolder.newFile();
            hdfsSite = configFolder.newFile();
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to create an temporary folder and file", e);
        }

        try (InputStream coreIs = new FileInputStream(coreSitePath)) {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document coreXml = documentBuilder.parse(coreIs);
            coreXml.getDocumentElement().normalize();
            NodeList properties = coreXml.getElementsByTagName("property");
            for (int i = 0; i < properties.getLength(); i++) {
                Node node = properties.item(i);
                node.normalize();
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    if (element.getElementsByTagName("name").item(0).getTextContent().equals("fs.defaultFS")) {
                        element.getElementsByTagName("value").item(0).setTextContent(String.format("hdfs://hadoop-master:%s", hostPortProvider.getHostPort(9000)));
                        break;
                    }
                }
            }

            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            Result output = new StreamResult(coreSite);
            Source input = new DOMSource(coreXml);
            transformer.transform(input, output);

            // Since hdfs-site.xml doesn't contain any port, so we can just copy it over
            Files.copy(new File(hdfsSitePath), hdfsSite);
        }
        catch (FileNotFoundException e) {
            throw new IllegalStateException(coreSitePath + " not found", e);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to create mock core-site.xml and hdfs-site.xml", e);
        }
        catch (ParserConfigurationException | SAXException | TransformerException e) {
            throw new IllegalStateException("Failed to load " + coreSitePath + " and " + hdfsSitePath, e);
        }

        // Update hive.properties file with the correct port
        try {
            hdfsSitePath = hdfsSite.getCanonicalPath();
            coreSitePath = coreSite.getCanonicalPath();
            properties.setProperty(TestConstantsHelper.HIVE_CONFIG_RESOURCES, coreSitePath + "," + hdfsSitePath);
        }
        catch (IOException e) {
            throw new IllegalStateException(coreSite + " and " + hdfsSite + " weren't created successfully", e);
        }
        properties.setProperty(TestConstantsHelper.HIVE_METASTORE_URI,
                String.format("thrift://hadoop-master:%s", hostPortProvider.getHostPort(9083)));
    }

    public Properties getHiveProperties()
    {
        return properties;
    }

    @Override
    public void close()
            throws IOException
    {
        // Try the best to keep the container running when there's still instance requiring it.
        // This is to avoid redundant container creation which takes a lot of time
        if (instanceCount.decrementAndGet() == 0) {
            dockerContainer.close();
        }
        if (configFolder != null) {
            configFolder.close();
        }
    }
}
