
Built-in System Access Control
==============================

A system access control plugin enforces authorization at a global level, before any connector level authorization. You can either use one of the built-in plugins in openLooKeng or provide your own by following the guidelines in [System Access Control](../develop/system-access-control.md). openLooKeng offers three built-in plugins:

| Plugin Name                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| `allow-all` (default value) | All operations are permitted.                                |
| `read-only`                 | Operations that read data or metadata are permitted, but none of the operations that write data or metadata are allowed. See "Read Only System Access Control" for details. |
| `file`                      | Authorization checks are enforced using a config file specified by the configuration property `security.config-file`. See ""File Based System Access Control" for details. |

Allow All System Access Control
-------------------------------

All operations are permitted under this plugin. This plugin is enabled by default.

Read Only System Access Control
-------------------------------

Under this plugin, you are allowed to execute any operation that reads data or metadata, such as `SELECT` or `SHOW`. Setting system level or catalog level session properties is also permitted. However, any
operation that writes data or metadata, such as `CREATE`, `INSERT` or `DELETE`, is prohibited. To use this plugin, add an `etc/access-control.properties` file with the following contents:

``` properties
access-control.name=read-only
```

File Based System Access Control
--------------------------------

This plugin allows you to specify access control rules in a file. To use this plugin, add an `etc/access-control.properties` file containing two required properties: `access-control.name`, which must be equal to `file`, and `security.config-file`, which must be equal to the location of the config file. For example, if a config file named `rules.json` resides in `etc`, add an `etc/access-control.properties` with the following contents:

``` properties
access-control.name=file
security.config-file=etc/rules.json
```

The config file is specified in JSON format.

-   It contains the rules defining which catalog can be accessed by which user (see Catalog Rules below).
-   The principal rules specifying what principals can identify as what users (see Principal Rules below).

This plugin currently only supports catalog access control rules and principal rules. If you want to limit access on a system level in any other way, you must implement a custom SystemAccessControl plugin (see [System Access Control](../develop/system-access-control.md).

### Refresh

By default, when a change is made to the `security.config-file`, openLooKeng must be restarted to load the changes. There is an optional property to refresh the properties without requiring a openLooKeng restart. The refresh period is specified in the `etc/access-control.properties`:

``` properties
security.refresh-period=1s
```

### Catalog Rules

These rules govern the catalogs particular users can access. The user is granted access to a catalog based on the first matching rule read from top to bottom. If no rule matches, access is denied. Each rule is
composed of the following fields:

-   `user` (optional): regex to match against user name. Defaults to `.*`.
-   `catalog` (optional): regex to match against catalog name. Defaults to `.*`.
-   `allow` (required): boolean indicating whether a user has access to the catalog


**Note**

*By default, all users have access to the `system` catalog. You can override this behavior by adding a rule.*


For example, if you want to allow only the user `admin` to access the `mysql` and the `system` catalog, allow all users to access the `hive` catalog, and deny all other access, you can use the following rules:

``` json
{
  "catalogs": [
    {
      "user": "admin",
      "catalog": "(mysql|system)",
      "allow": true
    },
    {
      "catalog": "hive",
      "allow": true
    },
    {
      "catalog": "system",
      "allow": false
    }
  ]
}
```

### Impersonation Rules

These rules control the ability of a user to impersonate another user. In some environments it is desirable for an administrator (or managed system) to run queries on behalf of other users. In these cases, the administrator authenticates using their credentials, and then submits a query as a different user. When the user context is changed, openLooKeng will verify the administrator is authorized to run queries as the target user.

When these rules are present, the authorization is based on the first matching rule, processed from top to bottom. If no rules match, the authorization is denied. If impersonation rules are not present but the legacy principal rules are specified, it is assumed impersonation access control is being handled by the principal rules, so impersonation is allowed. If neither impersonation nor principal rules are defined, impersonation is not allowed.

Each impersonation rule is composed of the following fields:

* `original_user` (required): regex to match against the user requesting the impersonation.
* `new_user` (required): regex to match against the user that will be impersonated.
* `allow` (optional): boolean indicating if the authentication should be allowed.

The following example allows the two admins, `alice` and `bob`, to impersonate any user, except they may not impersonate each other. It also allows any user to impersonate the `test` user:

```json
{
    "impersonation": [
        {
            "original_user": "alice",
            "new_user": "bob",
            "allow": false
        },
        {
            "original_user": "bob",
            "new_user": "alice",
            "allow": false
        },
        {
            "original_user": "alice|bob",
            "new_user": ".*"
        },
        {
            "original_user": ".*",
            "new_user": "test"
        }
    ]
}
```

### Principal Rules

**Warning**

* Principal rules are deprecated and will be removed in a future release. These rules have been replaced with [Authentication User Mapping](./user-mapping.md), which specifies how a complex authentication user name is mapped to a simple user name for openLooKeng, and impersonation rules defined above.

These rules serve to enforce a specific matching between a principal and a specified user name. The principal is granted authorization as a user
based on the first matching rule read from top to bottom. If no rules are specified, no checks will be performed. If no rule matches, user authorization is denied. Each rule is composed of the following fields:

-   `principal` (required): regex to match and group against principal.
-   `user` (optional): regex to match against user name. If matched, it will grant or deny the authorization based on the value of `allow`.
-   `principal_to_user` (optional): replacement string to substitute against principal. If the result of the substitution is same as the user name, it will grant or deny the authorization based on the
    value of `allow`.
-   `allow` (required): boolean indicating whether a principal can be authorized as a user.


**Note**

*You would at least specify one criterion in a principal rule. If you specify both criteria in a principal rule, it will return the desired conclusion when either of criteria is satisfied.*

The following implements an exact matching of the full principal name for LDAP and Kerberos authentication:

``` json
{
  "catalogs": [
    {
      "allow": true
    }
  ],
  "principals": [
    {
      "principal": "(.*)",
      "principal_to_user": "$1",
      "allow": true
    },
    {
      "principal": "([^/]+)(/.*)?@.*",
      "principal_to_user": "$1",
      "allow": true
    }
  ]
}
```

If you want to allow users to use the  extactly same name as their Kerberos principal name, and allow `alice` and `bob` to use a group principal named as `group@example.net`, you can use the following rules.

``` json
{
  "catalogs": [
    {
      "allow": true
    }
  ],
  "principals": [
    {
      "principal": "([^/]+)/?.*@example.net",
      "principal_to_user": "$1",
      "allow": true
    },
    {
      "principal": "group@example.net",
      "user": "alice|bob",
      "allow": true
    }
  ]
}
```

### Node State Rules
These rules govern the node state info particular users can access. The user is granted access to update a node state based on the first matching rule read from top to bottom. If no rule matches, access is denied. Each rule is
composed of the following fields:

- `user` (optional): regex to match against user name. Defaults to `.*`.
- `allow` (required): boolean indicating whether a user has access to the catalog

**Note**

*By default, all users have no access to update the node state info. You can override this behavior by adding a rule.*

For example, if you want to allow only the user `admin` and `alice` to update the node state, and deny all other access, you can use the following rules:

``` json
{
  "nodeInfo": [
    {
      "user": "admin",
      "allow": true
    },
    {
      "user": "alice",
      "allow": true
    },
    {
      "user": "bob",
      "allow": false
    }
  ]
}
```

### Heuristic Index Rules

The rules govern the Heuristic Index operations particular users can perform.

Each rule is composed of the following fields:

- `user` (required): regex to match against user name. Defaults to `.*`.
- `privileges` (optional): list of privileges granted to user (`ALL`, `SHOW`, `CREATE`, `DROP`, `RENAME`, and `UPDATE`). Defaults to `ALL`.

For example, here user `tom` can only execute `SHOW INDEX` or `CREATE INDEX` statements.
But user `admin` can execute all statements `CREATE INDEX`, `SHOW INDEX`, `DROP INDEX`, etc.

```json
{
  "indexAccess": [
    {
      "user": "tom",
      "privileges": ["SHOW", "CREATE"]
    },
    {
      "user": "admin",
      "privileges": ["ALL"]
    }
  ]
}
```