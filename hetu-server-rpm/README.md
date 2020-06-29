# openLooKeng RPM

## RPM Package Build And Usage

You can build an RPM package for openLooKeng server and install openLooKeng using the RPM. Thus, the installation is easier to manage on RPM-based systems.

The RPM builds by default in Maven, and can be found under the directory `hetu-server-rpm/target/`

The RPM has a pre-requisite of Python >= 2.4. It also needs Oracle Java 1.8 update 40 (8u40 64-bit) pre-installed. The RPM installation will fail if any of these requirements are not
satisfied.

To install openLooKeng using an RPM, run:

    rpm -i hetu-server-<version>-1.0.x86_64.rpm

This will install openLooKeng in single node mode, where both coordinator and workers are co-located on localhost. This will deploy the necessary default configurations along with a service script to control the openLooKeng server process.

Uninstalling the RPM is like uninstalling any other RPM, just run:

    rpm -e hetu

Note: During uninstall, any openLooKeng related files deployed will be deleted except for the openLooKeng logs directory `/var/log/hetu`.

## Control Scripts

The openLooKeng RPM will also deploy service scripts to control the openLooKeng server process. The script is configured with chkconfig,
so that the service can be started automatically on OS boot. After installing openLooKeng from the RPM, you can run:

    service hetu [start|stop|restart|status]

## Installation directory structure

We use the following directory structure to deploy various openLooKeng artifacts.

* /usr/lib/hetu/lib/: Various libraries needed to run the product. Plugins go in a plugin subdirectory.
* /etc/hetu: General openLooKeng configuration files like node.properties, jvm.config, config.properties. Connector configs go in a catalog subdirectory
* /etc/hetu/env.sh: Java installation path used by openLooKeng
* /var/log/hetu: Log files
* /var/lib/hetu/data: Data directory
* /usr/shared/doc/hetu: Docs
* /etc/rc.d/init.d/hetu: Control script

The node.properties file requires the following two additional properties since our directory structure is different from what standard openLooKeng expects.

    catalog.config-dir=/etc/hetu/catalog
    plugin.dir=/usr/lib/hetu/lib/plugin
