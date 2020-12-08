# openLooKeng ODBC用户手册

## 总览

### 介绍

本用户手册包含Windows版本的openLooKeng ODBC驱动程序的相关信息。包括驱动的安装与ODBC数据源的配置以及驱动的基本信息。

ODBC(Open Database Connectivity  开放数据连接)是微软提出的一个能让应用访问不同DBMS的互操作接口协议，其定义了一个通用的数据库访问机制并提供了一组访问数据库的应用程序编程接口(ODBC API)以简化客户端和不同DBMS间的互操作。

ODBC驱动为应用提供了连接到数据库的能力，本产品为openLooKeng的ODBC驱动程序，符合ODBC 3.5的核心级（Core Level）一致性规范。

### 先决条件

**使用本产品需要具备以下知识：**

* ANSI结构化查询语言（SQL）

* [ODBC程序员参考文档](https://docs.microsoft.com/en-us/sql/odbc/reference/odbc-programmer-s-reference?view=sql-server-ver15)

### 支持的版本

**本产品支持以下版本:**

- Winodows 10 64bit

- Winodows Server 2016 64bit

> 其他版本的Windows系统未经过严格测试，用户可以自己尝试，本产品不做任何质量保证



## openLooKeng ODBC驱动的安装

本手册对应二进制文件分发即msi安装包的安装形式，需要从源码编译安装的开发者请参考开源代码中的Build.md

### 配置需求

安装本驱动的系统，需要满足如下要求：

* 请确保系统为如下系统：
  * Windows 10 (64 bit)
  * Windows Server 2016 (64 bit)
  
* 请确保安装磁盘具有超过180MB的可用磁盘空间

### 安装步骤

安装本驱动前请确认当前具有管理员权限。

1. 双击msi安装包，出现安装的欢迎界面，单击“Next”。
2. 第二页为用户协议，勾选接受后单击“Next”。
3. 第三页选择安装方式，建议选择“Complete”完整安装。
4. 第四页选择安装路径，配置后单击“Next”。
5. 完成上述安装设置后在最后一页单击“Install”开始安装。

> 注意安装过程中会弹出cmd命令行窗口，为安装Driver组件的过程，待其完成后会自动关闭。至此完成了openLooKeng ODBC驱动的全部安装过程

6. 完成后出现弹出窗口，如果以前配置过旧版本驱动的用户DSN可以勾选使DSN用于新安装的版本，点击“Finish”完成安装



## 配置数据源

应用在使用openLooKeng ODBC驱动前需要在系统的ODBC数据源管理器中配置好数据源DSN

### 打开ODBC数据源管理器(64位)

1. 点击开始菜单，单击“控制面板”。

2. 在控制面板中单击“系统和安全”，找到并单击“管理工具”

3. 在“管理工具”中，找到并单击“ODBC数据源(64位)”

   > 注：也可以通过在Windows 10开始菜单的搜索框中键入“ODBC”，点击提示的“ODBC数据源(64位)”打开

### 添加用户DSN

1. 在打开的ODBC数据源管理器(64位)中单击用户DSN选项卡，单击“添加”按钮

2. 在弹出的创建新数据源窗口中找到openLooKeng ODBC Driver,  单击“完成”

3. 此时弹出了openLooKeng ODBC Driver配置的UI界面， 在第1页欢迎页面的Name输入框中输入想要创建的DSN的名字，在Description输入框中输入对该DSN的附加描述，单击“Next”按钮

4. 第2页主要包括如下六个输入框，功能和用法如下：

   | 输入框         | 说明                                                         |
   | -------------- | ------------------------------------------------------------ |
   | Connect URL    | 在此处输入要连接的openLooKeng服务器的IP地址和端口号          |
   | Connect Config | 在通过连接配置文件设置连接参数时，在这里传入配置文件的路径。可以通过Browse按钮选择 |
   | User Name      | 连接到openLooKeng的用户名，默认为root                        |
   | Password       | 用户密码，可以为空                                           |
   | Catalog        | 指定DSN要使用的Catalog，可以为空，建议在填好上方输入框单击Test DSN成功后从下拉框中选择 |
   | Schema         | 指定DSN要使用的Schema，可以为空，建议在填好上方输入框单击Test DSN成功后从下拉框中选择 |

   完成第2页各输入框的填写并单击“Test DSN”按钮显示成功信息后，单击“Next”按钮

5. 第3页Statement(s)输入框中可以输入在建立到openLooKeng服务器的连接后发送的初始语句，勾选“Debug”后驱动会在%TMP%路径下创建名为“MAODBC.LOG”的调试日志文件，记录openLooKeng ODBC Driver的调试信息，在第二页单击“Test DSN”成功后可以从“Character Set”下拉框中选择需要配置的连接字符集。勾选“Enable automatic reconnect”，在发送消息时如果连接已失效，将会自动与server重建连接(本功能无法对事务一致性提供保障，使用须谨慎)。最后在本页单击“Finish”按钮完成DSN的配置与添加。

### DSN配置ODBC连接

配置DSN时可以通过提供正确的Connect URL、User Name和Password建立基本的ODBC连接，而对于有诸如SSL、Kerberos等高级需求的用户，应当通过传入连接配置文件的方式将对应的连接参数传给驱动

### 连接配置文件

通过连接配置文件传入连接参数时，支持配置openLooKeng JDBC定义的所有连接参数，参见openLooKeng JDBC驱动的[参数参考](jdbc.md#参数参考)

连接配置文件中需要以文本形式提供一组按行分隔如user=root或SSL=true这样的参数键值，驱动会自动对其解析并完成对连接的配置，下面给出一个示例：

```
#文件路径分隔符请使用“\\”或“/”
user=root

#password=123456

# 是否使用HTTPS连接，默认false
SSL=true

# Java Keystore文件路径
#SSLKeyStorePath

# Java KeyStore密码
#SSLKeyStorePassword

# Java TrustStore文件路径
SSLTrustStorePath=F:/openLooKeng/hetuserver.jks

# Java TrustStore密码
#SSLTrustStorePassword

# Kerberos服务名称，固定为HTTP
KerberosRemoteServiceName=HTTP

# Kerberos principal
KerberosPrincipal=test

# 访问数据源用户的krb5配置文件
KerberosConfigPath=F:/openLooKeng/krb5.conf

# 访问数据源用户的keytab配置文件
KerberosKeytabPath=F:/openLooKeng/user.keytab
```

完成上述配置后单击“Test DSN”，配置正确的话驱动会弹出连接成功建立的对话框，建议用户可以通过检查点击Catalog和Schema的下拉框能否正确显示对应的Catalog和Schema确认连接的正确性，完成数据源DSN的配置后，ODBC应用就可以通过该配置好的DSN连接到openLooKeng



## 驱动支持的数据类型

本驱动支持的数据类型与对应的ODBC数据类型如下：

| openLooKeng数据类型   |       ODBC数据类型         |
| :------------------: | :----------------------: |
|    `BOOLEAN`         |    `SQL_BIT`             |
|    `TINYINT`         |    `SQL_TINYINT`         |
|    `SMALLINT`        |    `SQL_SMALLINT`        |
|    `INTEGER`         |    `SQL_INTEGER`         |
|    `BIGINT`          |    `SQL_BIGINT`          |
|    `REAL`            |    `SQL_REAL`            |
|    `DOUBLE`          |    `SQL_DOUBLE`          |
|    `DECIMAL`         |    `SQL_DECIMAL`         |
|    `CHAR`            |    `SQL_CHAR`            |
|    `VARCHAR`         |    `SQL_VARCHAR`         |
|    `VARBINARY`       |    `SQL_VARBINARY`       |
|    `DATE`            |    `SQL_TYPE_DATE`       |
|    `TIME`            |    `SQL_TYPE_TIME`       |
|    `TIMESTAMP`       |    `SQL_TYPE_TIMESTAMP`  |
|`INTERVAL YEAR TO MONTH`|  `SQL_VARCHAR`         |
|`INTERVAL DAY TO SECOND`|  `SQL_VARCHAR`         |
|`ARRAY`| `SQL_VARCHAR` |
|`MAP`| `SQL_VARCHAR` |
|`ROW`| `SQL_VARCHAR` |
|`JSON`| `SQL_VARCHAR` |

数据类型的详细信息用户可以通过调用Catalog Functions中的SQLGetTypInfo获得

## 字符集

本驱动同时支持ANSI与Unicode应用，默认的连接字符集对于ANSI应用为系统默认字符集，对于Unicode应用为utf8编码。如果应用使用的字符集与上述默认字符集不同可能会出现乱码，对此用户应该指定连接字符集使之与应用所需的字符集相适应。下面对连接字符集的相应配置进行说明。

在调用ODBC API获取数据时，若绑定到SQL_C_WCHAR类型的缓冲区，无论对ANSI还是Unicode应用驱动都会返回Unicode编码的结果，而绑定到SQL_C_CHAR类型的缓冲区时在默认情况下驱动会对ANSI应用返回按系统默认字符集编码的结果，对Unicode应用会返回按utf8编码的结果。若应用采用的编码与默认不符，可能会造成结果乱码，为此用户应通过配置连接字符集以指定结果的编码。例如若应用出现中文乱码的情况，可以尝试将连接字符集配置为GBK或者GB2312。

当前本驱动所支持的所有连接字符集都可以在配置数据源的UI界面第三页中的“Character Set”下拉框中进行设置，用户可以在“Test DSN”成功后跟据自身需求从下拉框中选择连接字符集。