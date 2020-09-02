
# Filesystem Access Utilities

## Overview

openLooKeng project includes a set of filesystem client utilities to help access and modifying files. Currently, two sets of filesystems are supported: HDFS and local filesystem. A ``HetuFileSystemClient`` interface is provided in the SPI, which defines the common file operations to be used in the project. The goal of this client is to provide unified interface, behaviors
and exceptions across different filesystems. Therefore client code can easily reuse codes and transfer their logic without having to change the code.

A utility class ``FileBasedLock`` located in SPI can provide exclusive access on a given filesystem. It takes advantage of the unified filesystem client interface thus works for different filesystems.

The filesystem clients are implemented as openLooKeng Plugins. The implementations are placed in the ``hetu-filesystem-client`` module, where one factory extending ``HetuFileSystemClientFactory`` must be implemented and registered in ``HetuFileSystemClientPlugin``.

## Filesystem Profiles
A filesystem profile must contain a type field:

    fs.client.type=<filesystem type>

Additional configs used by the corresponding filesystem client can be provided. For example, hdfs filesystem needs paths to config resource files ``core-site.xml`` and ``hdfs-site.xml``. If the filesystem enables authentication, such as KERBEROS, credentials should be specified in the profile as well.

Multiple profiles defining filesystem clients can be placed in ``etc/filesystem``.  Similar to catalogs, they will be loaded upon server start by a filesystem manager which also produce filesystem clients to users in ``presto-main``. Client modules can read profiles in this folder by themselves, but it is highly recommended that client modules obtain a client which is provided by the main module
in order to prevent dependency issues.

A typical use case of having multiple profiles is when multiple hdfs are used and preset. In this case create a property file for each cluster and include their own authentication information in different profiles, for example ``hdfs1.properties`` and ``hdfs2.properties``. Client code will be able to access them by specifying the profile name (file name), such as ``getFileSystemClient("hdfs1", <rootPath>)`` from the ``FileSystemClientManager``.

For each local and hdfs filesystem client, it requires a root path to which the access to filesystem is limited. All access beyond this root dir will be denied. It is suggested to use the deepest directory that meets the need as the client root, in order to avoid the risk of modifying (or deleting) additional files by accident.

### Filesystem profile properties

| Property Name                | Mandatory                        | Description                                                  | Default Value  |
| ---------------------------- | -------------------------------- | ------------------------------------------------------------ | -------------- |
| `fs.client.type`             | YES                              | The type of filesystem profile. Accepted values: `local`, `hdfs`. |                |
| `hdfs.config.resources`      | NO                               | Path to hdfs resource files (e.g. core-site.xml, hdfs-site.xml) | Use Local hdfs |
| `hdfs.authentication.type`   | YES                              | hdfs authentication Accepted values: `KERBEROS`, `NONE`      |                |
| `hdfs.krb5.conf.path`        | YES if auth type set to KERBEROS | Path to the krb5 config file                                 |                |
| `hdfs.krb5.keytab.path`      | YES if auth type set to KERBEROS | Path to the kerberos keytab file                             |                |
| `hdfs.krb5.principal`        | YES if auth type set to KERBEROS | Principal of kerberos authentication                         |                |
| `fs.hdfs.impl.disable.cache` | NO                               | Disable cache in hdfs.                                       | `false`        |

openLooKengFileSystemClient

The unified ``HetuFileSystemClient`` sets the standard of filesystem access for:
- Method signatures
- Behaviors
- Exceptions

The ultimate goal of doing all these unification is to increase the reusability of code that needs to perform file operations on multiple filesystem.

### Method signatures and behaviors

The methods follow the same signature as those in ``java.nio.file.spi.FileSystemProvider`` and ``java.nio.file.Files`` in order to maximize compatibility. There are also additional methods that are not available in ``java.nio`` package to supply useful functionalities (e.g. ``deleteRecursively()``).

### Exceptions

Similar to method signatures, exception patterns strictly follow the same as those produced in ``Files``. For other filesystems, the implementation translates exceptions to Java local ``java.nio.file.FileSystemException`` which is most semantically equivalent.
This allows the client code to:

1. handle ``IOException``  without worrying about the underlying filesystem. 
2. decouple from extra dependencies, such as hadoop

Here are examples of exceptions that are translated from hdfs:

- ``org.apache.hadoop.fs.FileAlreadyExistsException`` -> ``java.nio.file.FileAlreadyExistsException``
- ``org.apache.hadoop.fs.PathIsNotEmptyDirectoryException`` -> ``java.nio.file.DirectoryNotEmptyException``

## FileBasedLock

A ``FileBasedLock`` utility is provided in ``io.prestosql.spi.filesystem`` to be used together with ``HetuFileSystemClient``.
This lock is file-based, and follows "try-and-back-off-on-exception" pattern to serialize operations in concurrent scenarios.

The lock design has a two-step file based lock schema: ``.lockFile`` to mark lock status and ``.lockInfo`` to
record lock ownership:

1. Before a client obtains the lock, it first checks if a valid (non-expire) ``.lockFile`` exists.

2. If not it tries to access the information stored in ``.lockInfo`` to see if the lock is held by itself, or try to write its uuid into the ``.lockInfo`` file if a valid file does not exist.

3. After all these it starts a background thread to keep updating ``.lockFile`` to inform others that the filesystem has been locked and renew it.

4. If any of the check above indicates that the lock has been acquired by others, or any exception occurs during the process, the current process/thread gives up (back-off) locking and tries again in the next loop.

