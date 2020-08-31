Assuming an openLooKeng docker image already exists, e.g. one built by `/hetu-core/docker/build-local.sh`.

Update the [deployment.yaml](./deployment.yaml) file and replace _all_ occurrences of `{{ imageTag }}` with the tag of the docker image.

### Deploy an openLooKeng cluster

```
kubectl create -f deployment.yaml
```

The cluster consists of 1 coordinator and 2 worker pods in the "openlk" namespace.

### Delete the openLooKeng cluster

```
kubectl delete -f deployment.yaml
```

### Additional configurations

To supply additional configuration files to be used (e.g. `config.properties` or `log.properties`):

- Store these files in a folder (this folder should only contain these configuration files)
- Locate comments that read `TODO: update to point to additional configuration and catalog folder`
- Update the `path` value to poin to the above folder
- Delete and redeploy openLooKeng cluster (the cluster must be deleted first for configuration changes to take place)

Notes:
- The above uses `hostPath` to supply configuration files from a mounted local drive. This only works when deploying to a _local_ Kubernetes cluster. To deploy to a remote Kubernetes cluster, configuration files may need to be supplied as Kubernetes `configMaps`.
- Ensure configuration files and subfolders have appropriate permissions to allow non-root users to read.

### Additional catalogs

To connect to data sources through additional catalog files:
- Place these files inside a `catalog` folder under the configuration file folder above
- Ensure `path` points to the configuration folder
- Delete and redeploy openLooKeng cluster

If there are other files that are needed by the new catalogs:
- Store them in a different folder (e.g. `etc`) under the above configuration folder (that is, parallel to `catalog`)
- Update references to these files (e.g. `my.file`) to `/custom-configs/<folder>/<file>` (e.g. `/custom-configs/etc/my.file`)

### Enable graceful shutdown

When an openLooKeng pod is shutdown, by default it's terminated immediately. To enable graceful shutdown, where the pod waits for all its active work to finish first:
- Locate comments that read `# TODO: enable if graceful shutdown is needed`
- Uncomment lines following the above comment
- Redeploy with

   ```
   kubectl apply -f deployment.yaml
   ```

With this change, it'll take at least a few minutes to kill any openLooKeng pod, because the graceful shutdown process needs to ensure no work is affected by shutting down the node. To shorten the wait period, the following line can be added to `config.properties`. See steps above about how to use custom configuration files.

```
shutdown.grace-period=10s
```

**Note**: only make this change in test/development environments.

### Enable auto-scaling

At the end of the [deployment.yaml](deployment.yaml) file, uncomment sections about `HorizontalPodAutoscaler` resources for coordinator or worker as appropriate, then redeploy.

Make other adjustments as needed, e.g. to include other metrics like memory usage, or change the target threshold.

By default, to ensure stability, scaling changes take at least 5 minutes to take effect. This can be configured through the `stabilizationWindowSeconds` field.