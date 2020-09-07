Assuming an openLooKeng docker image already exists, e.g. one built by `/hetu-core/docker/build-local.sh`.

Update the [deployment.yaml](./deployment.yaml) file and replace _all_ occurrences of `{{ imageTag }}` with the tag of the docker image.

### Deploy an openLooKeng cluster

```
docker stack deploy -f deployment.yaml <stack_name>
```

The cluster consists of 1 coordinator and 2 worker containers in the "openlk1" network.

### Delete the openLooKeng cluster

```
docker stack rm <stack_name>
```

### Additional configurations
- Need a "File manager" that helps docker swarm host the new configuration files among the swarm environment, since docker swarm doesn't have that service built-in
- New configuration files must be accessible as soon as a container starts (need to make sure files are mounted as if one is using docker swarm `config`)

To supply additional configuration files to be used (e.g. `config.properties` or `log.properties`):

- Store these files in a folder (this folder should only contain these configuration files)
- Mount the folder of new configuration files to each container (do it in the way required by your "File Manager")
- Inform openLooKeng containers where to look for the mounted folder using flag `-configDir` in the `command` list
- Delete and redeploy openLooKeng cluster (the cluster must be deleted first for configuration changes to take place)

### Additional catalogs

To connect to data sources through additional catalog files:
- Place these files inside a `catalog` folder under the configuration file folder above
- Inform openLooKeng containers where to look for the mounted folder using flag `-configDir` in the `command` list
- Delete and redeploy openLooKeng cluster

If there are other files that are needed by the new catalogs:
- Store them in a different folder (e.g. `etc`) under the above configuration folder (that is, parallel to `catalog`)
- Update references to these files (e.g. `my.file`) to `/custom-configs/<folder>/<file>` (e.g. `/custom-configs/etc/my.file`)
