
Ranger 访问控制
==============================

概要
-------------------------

Apache Ranger 为 Hadoop 集群提供了一种全面的安全框架，以跨组件的、一致性的方式进行定义、授权、管理安全策略。Ranger 详细介绍和用户指导可以参考[Apache Ranger Wiki](https://cwiki.apache.org/confluence/display/RANGER/Index )。

[openlookeng-ranger-plugin](https://gitee.com/openlookeng/openlookeng-ranger-plugin) 是为 openLooKeng 开发的 Ranger 插件，用于全面的数据安全监控和权限管理。

编译过程
-------------------------

1. 从 Git 仓库检出 [openlookeng-ranger-plugin](https://gitee.com/openlookeng/openlookeng-ranger-plugin) 代码

2. 进入代码根目录，执行 Maven 命令：

```
mvn clean package
```

3. 在上述 Maven 命令执行完成后，可以在 target 目录发现以下 TAR 文件：

```
ranger-<ranger.version>-admin-openlookeng-<openlookeng.version>-plugin.tar.gz
ranger-<ranger.version>-openlookeng-<openlookeng.version>-plugin.tar.gz
```

部署过程
-------------------------

### 安装 Ranger Admin 插件

1). 解压 ranger-&lt;ranger.version&gt;-admin-openlookeng-&lt;openlookeng.version&gt;-plugin.tar.gz，可以发现以下目录：

```
openlookeng
service-defs
```

2). Ranger 服务类型定义的注册

使用 Ranger Admin 提供的 REST API 向 Ranger 注册服务类型定义。注册后，Ranger Admin 将提供 UI 以创建服务实例（在以前的版本中称为存储库）和服务类型策略。Ranger 插件使用服务类型定义和策略来确定请求是否有访问权限以进行授权。如下示例所示，可以使用 curl 命令调用 REST API 接口：

```
curl -u admin:password -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @service-defs/ranger-servicedef-openlookeng.json http://ranger-admin-host:port/service/plugins/definitions
```

3). 复制 openlookeng 目录到 Ranger Admin 安装目录下的 ranger-plugins 目录 (例子：ranger-&lt;ranger.version&gt;-admin/ews/webapp/WEB-INF/classes/ranger-plugins/)

### 安装 openLooKeng 插件

1). 解压 ranger-&lt;ranger.version&gt;-openlookeng-&lt;openlookeng.version&gt;-plugin.tar.gz

2). 适当的修改 install.properties 文件。如下示例所示，修改了部分参数值：

> ```properties
> # Location of Policy Manager URL
> # Example: POLICY_MGR_URL=http://policymanager.xasecure.net:6080
> POLICY_MGR_URL=http://xxx.xxx.xxx.xxx:6080
> 
> # This is the repository name created within policy manager
> # Example: REPOSITORY_NAME=openlookengdev
> REPOSITORY_NAME=openlookengdev
>
> # openLooKeng component installed directory
> # COMPONENT_INSTALL_DIR_NAME=../openlookeng
> COMPONENT_INSTALL_DIR_NAME=/home/hetu-server-1.0.1
>
> XAAUDIT.SOLR.ENABLE=false
> XAAUDIT.SUMMARY.ENABLE=false
> ```

3). 执行 ./enable-openlookeng-plugin.sh

### 重启服务

```
重启 Ranger Admin 服务：service ranger-admin restart
重启 openLooKeng 服务：./launcher restart
```

Ranger 服务管理
-------------------------

通过点击 Ranger Admin 服务管理界面的 `OPENLOOKENG` 列旁的加号来新增 openLooKeng 服务。目前服务支持的配置参数如下所示：

| 标签                 | 描述                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Service Name | 服务的名称，需要在 openLooKeng 插件的 install.properties 配置文件中指定该服务名称 |
| Display Name | 服务显示的名称 |
| Description | 提供服务说明以供参考 |
| Active Status| 可以选择此选项来启用或禁用该服务 |
| Username | 指定可用于连接的用户名 |
| Password | 添加上述用户名对应的密码 |
| jdbc.driverClassName | 指定用于 openLooKeng 连接的驱动程序的完整类名。默认类名为：io.hetu.core.jdbc.OpenLooKengDriver |
| jdbc.url | jdbc:lk://host:port/catalog |
| Add new configurations | 指定其他任意配置项 |

Ranger策略管理
-------------------------

可以通过 Ranger Admin 的 openLooKeng 服务策略列表界面新增策略。openLooKeng 的 Ranger 插件目前支持 systemproperty、catalog、sessionproperty、schema、table 和 column 等资源的权限管理。

###  权限声明

- systemproperty
    - alter：设置系统会话属性。
- catalog
    - use：如果 catalog 没授予 use，所有 catalog 下操作都无权限。
    - select：只有授予 select 权限，show catalogs 才能显示对应的 catalog。
    - create：新建 schema。
    - show：显示当前 catalog 或者指定 catalog 下的所有授权 schema。
- sessionproperty
    - alter：设置 catalog 会话属性。
- schema
    - drop：删除已有 schema。
    - alter：修改已有 schema 的定义。
    - select：只有授予 select 权限，show schemas 才能显示对应的 schema。
    - create：使用给定列新建表；通过 select 查询语句来新建视图。
    - show：显示当前 schema 或者指定 schema 下的所有授权表。
- table
    - drop：删除已有表、视图、列。
    - alter：修改已有表的定义；设置表声明；更新给定条件下的行数据。
    - insert：插入行数据。
    - delete：删除行数据。
    - grant：给特定授权人授予特定权限。
    - revoke：取消特定授权人的特定权限。
    - select：只有授予 select 权限，show tables 才能显示对应的表。
    - show：显示指定表下的所有授权列的类型和其他属性。
- column
    - select：检索数据；只有授予 select 权限，show columns 才能显示对应的列。

###  策略配置参考

- 基本策略：必须给相应的 catalog 授权 `use` 和 `select` 权限。catalog 的 `show` 和 `create` 权限作为可选项以确定是否启用 show catalogs 和创建 schema 的权限。

| 标签                 | 描述                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| 策略名称 | 输入策略名称。|
| catalog | 选择相应的 catalog。|
| none | 无需继续配置其他资源。|

- 如果需要获取 catalog 的元数据信息 (例如：show schemas/tables/columns)，需要给相应 catalog 下的 `information_schema` 授予 `select` 权限。

| 标签                 | 描述                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | 输入策略名称。|
| catalog | 选择相应的 catalog。|
| schema | 选择 `information_schema`。|
| table  | * (表示选择所有表) |
| column | * (表示选择所有列) |

- 新增系统会话属性权限管理策略。

| 标签                 | 描述                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | 输入策略名称。|
| systemproperty | 设定相应的系统会话属性。|

- 新增 catalog 会话属性权限管理策略。

| 标签                 | 描述                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | 输入策略名称。|
| catalog | 选择相应的 catalog。|
| sessionproperty | 设定相应的 catalog 会话属性。|

- 新增含 catalog、schema、table 和 column 组合的权限管理策略。

| 标签                 | 描述                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | 输入策略名称。|
| catalog | 选择相应的 catalog。|
| schema | 对于选中的 catalog(s)，选择该策略适用的 schema(s)。|
| table  | 对于选中的 catalog(s) 和 schema(s)，选择该策略适用的表。|
| column | 对于选中的 catalog(s)、schema(s) 和表，选择该策略适用的列。|
