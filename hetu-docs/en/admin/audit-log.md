# Audit Log

openLooKeng audit logging functionality is a custom event listener that is invoked for query creation and query completion (success or failure)
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
```

Other audit logging properties include: 

`hetu.event.listener.audit.file`: Optional property to define absolute file path for the audit file. Ensure the process running the openLooKeng server has write access to this directory.

`hetu.event.listener.audit.filecount`: Optional property to define the number of files to use

`hetu.event.listener.audit.limit`: Optional property to define the maximum number of bytes to write to any one file

Example configuration file:

``` properties
event-listener.name=hetu-listener
hetu.event.listener.type=AUDIT
hetu.event.listener.listen.query.creation=true
hetu.event.listener.listen.query.completion=true
hetu.event.listener.audit.file=/var/log/hetu/hetu-audit.log
hetu.event.listener.audit.filecount=1
hetu.event.listener.audit.limit=100000
```
