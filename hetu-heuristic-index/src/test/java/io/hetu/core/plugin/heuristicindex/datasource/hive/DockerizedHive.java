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
package io.hetu.core.plugin.heuristicindex.datasource.hive;

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
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;

/**
 * <pre>
 * Class that manages a hive docker container.
 * The following requirements must be met in order to run this class:
 * 1. The current user must have permission to use Docker without sudo
 * 2. The Hive image must be loaded into Docker: docker pull hetudev/hdp2.6-hive:13
 * The test class will try to load this image from docker hub, but there may be proxy issues
 * 3. Need to make sure hadoop-master is mapped to 127.0.0.1 in /etc/hosts setting.
 * </pre>
 */
public class DockerizedHive
        implements Closeable
{
    private ImmutableList<Integer> ports = ImmutableList.of(1180, 8020, 8042, 8088, 9000, 9083, 10000, 19888, 50070, 50075);
    // make sure docker port 9000 is mapped to 9000 on host because docker container will always return 9000 when asked
    // for HDFS port as the container doesn't know there is an extra network layer by docker
    private ImmutableList<Integer> fixedPorts = ImmutableList.of(9000);

    private static final String DOCKER_HIVE_HOST_NAME = "hadoop-master";
    private static final String DOCKER_HIVE_NETWORK_NAME = "test_docker_hive";
    private static final String HDFS_AUTHENTICATION_KERBEROS = "KERBEROS";

    public static final String DATABASE_NAME = "unit_test";
    public static final String TABLE_NAME = "orc_data_source_test";
    public static final String PARTITIONED_TABLE_NAME = "orc_data_source_test_partitioned";
    public static final String TYPES_TABLE_NAME = "orc_types_test"; // used to test different hive column types
    public static final String TYPES_TMPT_TABLE_NAME = "orc_types_dummy"; // used to insert struct value

    private static DockerizedHive instance;

    private static AtomicInteger instanceCount = new AtomicInteger(0);

    private DockerContainer dockerContainer;

    private Configuration config;

    private Properties properties;

    private TempFolder configFolder;

    private String hdfsSitePath = TestConstantsHelper.DOCKER_HDFS_SITE_FILE;

    private String coreSitePath = TestConstantsHelper.DOCKER_CORE_SITE_FILE;

    private String hivePropertiesPath = TestConstantsHelper.DOCKER_HIVE_PROPERTIES_FILE_LOCATION;

    private DockerContainer.HostPortProvider hostPortProvider;

    private FileSystem fs;

    private String hiveJdbcUri;

    public static synchronized DockerizedHive getInstance(String testName)
            throws IOException
    {
        instanceCount.getAndIncrement();
        if (instance == null) {
            try {
                instance = new DockerizedHive();
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
        return instance;
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
                fixedPorts,
                Collections.emptyMap(),
                DOCKER_HIVE_HOST_NAME,
                DOCKER_HIVE_NETWORK_NAME),
                this::healthCheck);
        createTable();
    }

    private void healthCheck(DockerContainer.HostPortProvider hostPortProvider)
            throws IOException, SQLException
    {
        // if the service is not up, this will throw an error
        this.hostPortProvider = hostPortProvider;
        FileSystem fs = getFs();
        fs.exists(new Path("/"));

        // check hive connection
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        Connection connection = DriverManager.getConnection(getHiveJdbcUri(), "hive", "");
        connection.close();
    }

    public void createTable()
    {
        String dataFilePath = TestConstantsHelper.TEST_FOLDER_PATH + "000.csv";
        String fileLocation = "/tmp/orc_test/";
        try (Connection con = DriverManager.getConnection(getHiveJdbcUri(), "hive", "")) {
            // Uploading csv file to HDFS first
            FileSystem fs = getFs();
            OutputStream out = fs.create(new Path(fileLocation + "data.csv"));
            try (FileInputStream fin = new FileInputStream(dataFilePath)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = fin.read(buffer)) > 0) {
                    out.write(buffer, 0, length);
                }
            }
            finally {
                out.close();
            }

            Statement stmt = con.createStatement();

            // Loading the csv data into a temp table
            String externalTableSql = String.format("CREATE EXTERNAL TABLE IF NOT EXISTS default.%s%n", "tempTable")
                    + "(name string, surname string, age int, phone_number string)" + lineSeparator()
                    + "ROW FORMAT DELIMITED" + lineSeparator()
                    + "FIELDS TERMINATED BY ','" + lineSeparator()
                    + "STORED AS TEXTFILE" + lineSeparator()
                    + String.format("LOCATION '%s'", fileLocation);
            stmt.execute(externalTableSql);

            // Create the test schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + DATABASE_NAME);

            // Create the test table by loading data from the temp table
            stmt.execute(String.format(
                    "CREATE TABLE %s.%s (name string, surname string, age int, phone_number string) STORED AS ORC",
                    DATABASE_NAME, TABLE_NAME));
            stmt.execute(String.format(
                    "INSERT INTO TABLE %s.%s SELECT * FROM default.tempTable", DATABASE_NAME, TABLE_NAME));

            // Create the partitioned test table
            stmt.execute(String.format(
                    "CREATE TABLE %s.%s_partitioned (name string, surname string, phone_number string) PARTITIONED " +
                            "BY(age int) STORED AS ORC", DATABASE_NAME, TABLE_NAME));
            stmt.execute("SET hive.exec.dynamic.partition=true");
            stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict");
            stmt.execute(String.format(
                    "INSERT INTO TABLE %s.%s_partitioned PARTITION (age) SELECT name, surname, phone_number, age from " +
                            "default.tempTable", DATABASE_NAME, TABLE_NAME));

            // Create types table
            stmt.execute(String.format(
                    "DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TYPES_TABLE_NAME));
            // Hive doesn't support insert data of complex types like struct using plain text
            // To insert complex types like struct, need to use a dummy table in the middle
            stmt.execute(String.format(
                    "CREATE TABLE %s.%s (" +
                            " t_rowTmp INT" +
                            ", t_lablename STRING" +
                            ", t_lablevalue INT" +
                            ", t_array1 STRING" +
                            ", t_array2 STRING" +
                            ") STORED AS ORC", DATABASE_NAME, TYPES_TMPT_TABLE_NAME));

            stmt.execute(String.format(
                    "CREATE TABLE %s.%s (" +
                            "  t_string STRING" +
                            ", t_tinyint TINYINT" +
                            ", t_smallint SMALLINT" +
                            ", t_int INT" +
                            ", t_bigint BIGINT" +
                            ", t_float FLOAT" +
                            ", t_double DOUBLE" +
                            ", t_boolean BOOLEAN" +
                            ", t_timestamp TIMESTAMP" +
                            ", t_binary BINARY" +
                            ", t_date DATE" +
                            ", t_varchar VARCHAR(50)" +
                            ", t_char CHAR(25)" +
                            ", t_decimal DECIMAL" +
                            ", t_row struct<t_col:INT>" +
                            ", t_array array<string>" +
                            ", t_lables map<string,INT>" +
                            ") STORED AS ORC", DATABASE_NAME, TYPES_TABLE_NAME));

            stmt.execute(String.format(
                    "INSERT INTO TABLE %s.%s VALUES (" +
                            "1234," +
                            "'tableName'," +
                            "678," +
                            "'string1'," +
                            "'string2'" +
                            ")", DATABASE_NAME, TYPES_TMPT_TABLE_NAME));

            stmt.execute(String.format(
                    "INSERT INTO TABLE %s.%s " +
                            "SELECT " +
                            "'one'," +
                            "2," +
                            "3," +
                            "4," +
                            "5," +
                            "6.0," +
                            "7.0," +
                            "true," +
                            "'2011-05-06 07:08:09.1234567'," +
                            "'binary'," +
                            "'2016-01-01'," +
                            "'twelve'," +
                            "'thirteen'," +
                            "'14.0', " +
                            "named_struct('t_col', t_rowTmp), " +
                            "array(t_array1,t_array2), " +
                            "map(t_lablename,t_lablevalue)" +
                            " from %s.%s", DATABASE_NAME, TYPES_TABLE_NAME, DATABASE_NAME, TYPES_TMPT_TABLE_NAME));
        }
        catch (IOException e) {
            close();
            throw new UncheckedIOException(e);
        }
        catch (SQLException e) {
            close();
            throw new IllegalStateException(e);
        }
    }

    private void checkHostnameResolution(String hostname)
    {
        try {
            InetAddress.getByName(hostname);
        }
        catch (UnknownHostException e) {
            throw new IllegalStateException(String.format(
                    "Make sure '127.0.0.1 %s' is added to /etc/hosts", DOCKER_HIVE_HOST_NAME), e);
        }
    }

    private void checkFileExist(String path)
    {
        File file = new File(path);
        Preconditions.checkArgument(file.exists(), file.getAbsolutePath() + " is not found");
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

    /**
     * Adjust the Hadoop Configuration object and hive properties object by creating two temporary mock core-site.xml
     * and hdfs-site.xml that contains the correct port to the container.
     */
    private void correctPortMapping()
    {
        // We'll create two temporary core-site.xml and hdfs-site.xml that contains the correct port to the
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
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setFeature("http://javax.xml.XMLConstants/feature/secure-processing", true);
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty("omit-xml-declaration", "yes");
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
            properties.setProperty(ConstantsHelper.HIVE_CONFIG_RESOURCES, coreSitePath + "," + hdfsSitePath);
        }
        catch (IOException e) {
            throw new IllegalStateException(coreSite + " and " + hdfsSite + " weren't created successfully", e);
        }
        properties.setProperty(ConstantsHelper.HIVE_METASTORE_URI,
                String.format("thrift://hadoop-master:%s", hostPortProvider.getHostPort(9083)));
    }

    public Properties getHiveProperties()
    {
        return properties;
    }

    public String getHiveJdbcUri()
    {
        if (hiveJdbcUri == null) {
            hiveJdbcUri = String.format("jdbc:hive2://hadoop-master:%s/default", hostPortProvider.getHostPort(10000));
        }
        return hiveJdbcUri;
    }

    @Override
    public void close()
    {
        if (instanceCount.decrementAndGet() == 0) {
            instance = null;
            dockerContainer.close();
        }
        if (configFolder != null) {
            configFolder.close();
        }
    }
}
