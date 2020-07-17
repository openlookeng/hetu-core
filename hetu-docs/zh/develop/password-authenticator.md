

# 密码验证器

openLooKeng 支持使用用户名和密码通过自定义密码验证器进行身份验证，该验证器验证凭据并创建一个主体。

## 实现

`PasswordAuthenticatorFactory` 负责创建一个 `PasswordAuthenticator` 实例。它还定义管理员在 openLooKeng 配置中使用的该验证器的名称。

`PasswordAuthenticator` 包含单个方法 `createAuthenticatedPrincipal()`，该方法验证凭据并返回一个 `Principal`，然后 `system-access-control` 对该 `Principal` 进行授权。

必须将 `PasswordAuthenticatorFactory` 的实现包装为一个插件并将其安装在 openLooKeng 集群上。

## 配置

在将实现 `PasswordAuthenticatorFactory` 的插件安装在协调节点上之后，使用 `etc/password-authenticator.properties` 文件对其进行配置。除 `access-control.name` 之外，所有属性都特定于 `PasswordAuthenticatorFactory` 实现。

`password-authenticator.name` 属性由 openLooKeng 用于根据 `PasswordAuthenticatorFactory.getName()` 返回的名称查找注册的 `PasswordAuthenticatorFactory`。其余的属性作为到 `PasswordAuthenticatorFactory.create()` 的映射进行传递。

配置文件示例：

``` properties
password-authenticator.name=custom-access-control
custom-property1=custom-value1
custom-property2=custom-value2
```

此外，必须将协调节点配置为使用密码验证并启用 HTTPS。