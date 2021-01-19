# 元数据存储
本节介绍openLooKeng元数据存储。元数据存储用于存储元数据信息, 当前元数据存储支持将元数据保存在RDBMS或HDFS中。

## 配置元数据存储

首先在etc目录下创建配置文件`hetu-metastore.properties`。

### 使用RDBMS存储

在配置文件`hetu-metastore.properties`增加如下配置：

``` properties
hetu.metastore.type=jdbc
hetu.metastore.db.url=jdbc:mysql://....
hetu.metastore.db.user=root
hetu.metastore.db.password=123456
```

上述属性说明如下：

- `hetu.metastore.type`：元数据存储类型，使用RDBMS存储时配置为`jdbc`。
- `hetu.metastore.db.url`：连接RDBMS的URL。
- `hetu.metastore.db.user` :连接RDBMS的用户名。 
- `hetu.metastore.db.password` :连接RDBMS的密码。

### 使用HDFS存储

在配置文件`hetu-metastore.properties`增加如下配置：

```
hetu.metastore.type=hetufilesystem
hetu.metastore.hetufilesystem.profile-name=hdfs-config-metastore
hetu.metastore.hetufilesystem.path=/etc/openlookeng/metastore
```

上述属性说明如下：

- `hetu.metastore.type`：元数据存储类型，使用HDFS存储时配置为`hetufilesystem`。
- `hetu.metastore.hetufilesystem.profile-name`：使用HDFS存储配置文件的名称。
- `hetu.metastore.hetufilesystem.path`：配置文件的路径。

可以从[文件系统](../develop/filesystem.md )中获取更多的文件系统相关的信息

### 元数据存储缓存

openLooKeng可以配置开启元数据缓存，开启后访问元数据时会将其缓存到内存中，再次访问时可以提高元数据访问效率。
在配置文件`hetu-metastore.properties`增加如下配置：

``` properties
hetu.metastore.cache.size=10000
hetu.metastore.cache.ttl=4h     
```

上述属性说明如下：

- `hetu.metastore.cache.size`：元数据缓存大小，默认10000。
- `hetu.metastore.cache.ttl`：缓存元数据的过期时间，默认值0 (元数据缓存关闭)。