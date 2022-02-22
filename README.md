# Sequences plugin

The Sequences plugin provides transactional sequences for GraphDB. A sequence is a long counter that can be atomically incremented in a transaction to provide incremental IDs.

See more information on its usage [here](https://graphdb.ontotext.com/documentation/9.11/enterprise/sequences-plugin.html).

See additional documentation on the GraphDB Plugin API 
[here](https://graphdb.ontotext.com/enterprise/plug-in-api.html). 

## Deployment

Below you can find the deployment steps for this plugin:

1. Build the project using `mvn clean package`
2. Unzip `./target/sequences-plugin-graphdb-plugin.zip` in  `<GDB_INST_DIR>/lib/plugins/`.

Once you start GraphDB and a repository is initialized you will see the following entries in the log:
```
Registering plugin sequences
Initializing plugin 'sequences'
```
