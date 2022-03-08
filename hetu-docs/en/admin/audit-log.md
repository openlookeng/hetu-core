# Audit Log

openLooKeng audit logging functionality is a custom event listener, which monitors the start and stop of openLooKeng cluster and the dynamic addition and deletion of nodes in the cluster; Listen to WebUi user login and exit events; Listen for query events and call when the query is created and completed (success or failure).
An audit log contains the following information:

1. time when an event occurs
2. user ID
3. address or identifier of the access initiator
4. event type (operation)
5. name of accessed resource
6. result of an event

In an openLooKeng cluster, only a single event listener plugin can be active at a time.

## Implementation

Audit logging is an implementation of `io.prestosql.spi.eventlistener.EventListener` in `HetuListener` plugin. Methods overwritten include `AuditEventLogger#onQueryCreatedEvent`
and `AuditEventLogger#onQueryCompletedEvent`.

## Configuration
To enable audit logging feature, the following configs must be present in `etc/event-listener.properties` for this feature to be active.

```
hetu.event.listener.type=AUDIT
hetu.event.listener.listen.query.creation=true
hetu.event.listener.listen.query.completion=true
hetu.auditlog.logoutput=/var/log/
hetu.auditlog.logconversionpattern=yyyy-MM-dd.HH
```

The following is a detailed description of audit logging properties:

`hetu.event.listener.type`: property to define logging type for audit files. Allowed values are AUDIT and LOGGER.

`hetu.auditlog.logoutput`: property to define absolute file directory for audit files. Ensure the process running the openLooKeng server has write access to this directory.

`hetu.auditlog.logconversionpattern`: property to define the conversion pattern of audit files. Allowed values are yyyy-MM-dd.HH and yyyy-MM-dd.

Example configuration file:

``` properties
event-listener.name=hetu-listener
hetu.event.listener.type=AUDIT
hetu.event.listener.listen.query.creation=true
hetu.event.listener.listen.query.completion=true
hetu.auditlog.logoutput=/var/log/
hetu.auditlog.logconversionpattern=yyyy-MM-dd.HH
```
