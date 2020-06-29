+++

weight = 9
title = "openLooKeng ODBC User Manual"
+++

# openLooKeng ODBC User Manual

## Overview

### Introduction

This user manual contains information about the openLooKeng ODBC driver for Windows, including driver installation, ODBC data source configuration, and basic driver information.

Open Database Connectivity (ODBC) is an interoperable interface protocol proposed by Microsoft for applications to access different DBMSs. It defines a universal database access mechanism and provides a set of ODBC APIs for accessing databases to simplify the interoperability between clients and different DBMSs.

The ODBC driver enables applications to connect to databases. This product is the ODBC driver for openLooKeng and complies with the core level consistency specifications of ODBC 3.x.

### Prerequisites

**The following knowledge is required for using this product:**

* ANSI structured query language (SQL)

* [ODBC Programmer's Reference](https://docs.microsoft.com/en-us/sql/odbc/reference/odbc-programmer-s-reference?view=sql-server-ver15)

### Supported Version

**This product supports the following version:**

- Windows 10 64-bit

> This product has not been strictly tested for other Windows versions. You can try them by yourself. This product does not provide any quality assurance.

## Installing the openLooKeng ODBC Driver

This document describes installing from binary distribution for ODBC driver, which is an MSI package. For developers who need to compile and install from the source code, please refer to **Build.md** along with the open-source code.

### System Requirements

* This driver provides only the 64-bit installation package. Ensure that the OS is Windows 10 64-bit.

* Ensure that the installation disk has more than 100 MB available space.

### Procedure

Before installing the driver, ensure that you have the administrator rights.

1. Double-click the **hetu-odbc-win64.msi** installation package. The welcome page is displayed. Click **Next**.
2. The second page is the user agreement. accept the terms and click **Next**.
3. On the third page, select an installation mode. You are advised to select **Complete**.
4. On the fourth page, select an installation path and click **Next**.
5. After the preceding installation settings are complete, click **Install** on the last page to start the installation.

> During the installation, the cmd window is displayed, showing the process of installing the driver components. After the installation is complete, the cmd window is automatically closed. The openLooKeng ODBC driver is installed.

6. In the dialog box that is displayed, use DSN for new installation if the user DSN has been configured with the driver of an earlier version, and click **Finish**.

## Configuring the Data Source

Before an application uses the openLooKeng ODBC driver, the data source DSN must be configured in the ODBC data source manager of the system.

### Opening the ODBC Data Source Administrator (64-bit)

1. Click **Start**, and choose **Control Panel**.

2. In **Control Panel**, click **System and Security**, and then click **Administrative Tools**.

3. In **Administrative Tools**, click **ODBC Data Sources (64-bit)**.
   
   > Note: You can also type **ODBC** in the search box of the Windows 10 **Start** menu and click **ODBC Data Sources (64-bit)** to open it.

### Adding the User DSN

1. In the ODBC Data Source Administrator (64-bit), click the **User DSN** tab and click **Add**.

2. In the displayed **Create New Data Source** dialog box, click **Hetu ODBC Driver** and click **Finish**.

3. The Hetu ODBC Driver configuration page is displayed. On the welcome page, enter the name of the DSN to be created in the **Name** text box, enter the additional description of the DSN in the **Description** text box, and click **Next**.

4. The second page contains the following six text boxes. The functions and usage are as follows:
   
   | Text Box| Description|
   |----------|----------|
   | Connect URL| IP address and port number of the openLooKeng server to be connected.|
   | Connect Config| Path of the configuration file when setting connection parameters through the connection configuration file. You can click **Browse** to select a path.|
   | User Name| User name for connecting to the openLooKeng. The default value is **root**.|
   | Password| User password, which can be left empty.|
   | Catalog| Catalog to be used by the DSN, which can be left empty. You are advised to click **Test DSN** in the upper text box and select a value from the drop-down list box.|
   | Schema| Schema to be used by the DSN, which can be left empty. You are advised to click **Test DSN** in the upper text box and select a value from the drop-down list box.|

   Set the parameters on the second page, and then click **Test DSN**. After the system displays a message indicating that the operation is successful, click **Next**.

5. In the **Statement(s)** text box on page 3, enter the initial statement sent after the connection to the openLooKeng server is established. After **Debug** is selected, the driver creates a debugging log file named **MAODBC.LOG** in **C:\\Users\\***current user***\\AppData\\Local\\Temp** to record openLooKeng ODBC driver debugging information. Click **Finish**.

### Configuring the ODBC Connection for the DSN

When configuring DSN, you can set up a basic ODBC connection by providing the correct **Connect URL**, **User Name**, and **Password**. For users with advanced requirements such as SSL and Kerberos, you need to transfer the corresponding connection parameters to the driver by importing the connection configuration file.

### Connection Configuration File

When connection parameters are transferred through the connection configuration file, all connection parameters defined by openLooKeng JDBC can be configured. For details, see [Parameter Reference#](https://openLooKeng.io/docs/current/installation/jdbc.html#parameter-reference) of openLooKeng JDBC Driver.

The connection configuration file must provide a group of parameter key-value pairs separated by lines, such as **user=root** or **SSL=true**. The driver automatically parses the parameter key values and configures the connection. The following is an example:

```
#Use "\\" or "/" to separate file paths
user=root

#password=123456

# Whether to use HTTPS connection, the default value is "false"
SSL=true

# Java Keystore file path
#SSLKeyStorePath

# Java KeyStore password
#SSLKeyStorePassword

# Java TrustStore file path
SSLTrustStorePath=F:/openLooKeng/hetuserver.jks

# Java TrustStore password
#SSLTrustStorePassword

# Kerberos service name, fixed at "HTTP" 
KerberosRemoteServiceName=HTTP

# Kerberos principal
KerberosPrincipal=test

# krb5 configuration file of the user accessing to the data source
KerberosConfigPath=F:/openLooKeng/krb5.conf

# keytab configuration file of the user accessing to the data source
KerberosKeytabPath=F:/openLooKeng/user.keytab
```

After the preceding configuration is complete, click **Test DSN**. If the configuration is correct, a dialog box is displayed, indicating that the connection is successfully established. You are advised to check whether the corresponding catalog and schema are correctly displayed in the **Catalog** and **Schema** drop-down list boxes. After the data source DSN is configured, the ODBC application can be connected to the openLooKeng through the configured DSN.

## Data Types Supported by the Driver

The following table lists the data types supported by the driver, ODBC data types, and openLooKeng data types.

| openLooKeng Data Type| ODBC Data Type|
|:----------:|:----------:|
| `BOOLEAN`| `SQL_BIT`|
| `TINYINT`| `SQL_TINYINT`|
| `SMALLINT`| `SQL_SMALLINT`|
| `INTEGER`| `SQL_INTEGER`|
| `BIGINT`| `SQL_BIGINT`|
| `REAL`| `SQL_REAL`|
| `DOUBLE`| `SQL_DOUBLE`|
| `DECIMAL`| `SQL_DECIMAL`|
| `CHAR`| `SQL_CHAR`|
| `VARCHAR`| `SQL_VARCHAR`|
| `VARBINARY`| `SQL_VARBINARY`|
| `DATE`| `SQL_TYPE_DATE`|
| `TIME`| `SQL_TYPE_TIME`|
| `TIMESTAMP`| `SQL_TYPE_TIMESTAMP`|
| `INTERVAL YEAR TO MONTH`| `SQL_VARCHAR`|
| `INTERVAL DAY TO SECOND`| `SQL_VARCHAR`|

You can obtain the details about data types by calling **SQLGetTypInfo** in **Catalog Functions**.