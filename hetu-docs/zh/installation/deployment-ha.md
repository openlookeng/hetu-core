
# 部署具有高可用（HA）的openLooKeng

openLooKeng HA解决协调节点单点故障问题。用户可以在任何协调节点上提交查询，以平衡工作负载。

## 安装HA

安装具有HA的openLooKeng要求集群中至少有2个协调节点。请确保协调节点上的时间一致性。请按照[手动部署openLooKeng](deployment.md)或[自动部署openLooKeng](deployment-auto.md)进行基本设置。

## 配置HA
当前状态存储被用于HA和动态过滤功能特性。

### 配置协调节点和工作节点属性
在所有协调节点上的`etc/config.properties`中添加如下配置。
``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
```
  
在所有工作节点上的`etc/config.properties`中添加如下配置。
``` properties
hetu.multiple-coordinator.enabled=true
```

上述属性说明如下：
- `hetu.multiple-coordinator.enabled`: 启用多个协调节点。
- `hetu.embedded-state-store.enabled`: 协调节点启用嵌入式状态存储。

说明：建议在所有协调节点（或至少3个）上启用嵌入式状态存储，以确保节点/网络关闭时,服务可以保持高可用性。

### 配置状态存储区
关于配置状态存储, 请参考[状态存储](../admin/state-store.md)。

## HA及反向代理

要尽可能的显示HA的优势，建议客户端(例如openLooKeng CLI, JDBC等)不直接连接特定的协调节点，而是通过反向代理连接多个协调节点，例如使用负载平衡器或者Kubernetes服务，这样即使某个协调节点无法正常工作，客户端也可以继续使用其它的协调节点。

### 反向代理要求

通过反向代理连接时，要求客户端在执行语句查询期间连接到同一协调器，确保在语句查询运行时客户端和协调器之间保持恒定心跳。这可以通过配置反向代理实现，例如Nginx的`ip_hash`。

### 配置反向代理 (Nginx)

请在Nginx配置文件中包含以下配置（nginx.conf）。

```
http {
    ...  # 用户自定义配置
    upstream backend {
        ip_hash;
        server <coordinator1_ip>:<coordinator1_port>;
        server <coordinator2_ip>:<coordinator2_port>;
        server <coordinator3_ip>:<coordinator3_port>;
        ...
    }

    server {
        ... # 用户自定义配置
        location / {
            proxy_pass http://backend;
            proxy_set_header Host <nginx_ip>:<nginx_port>;
        }
    }
}
```