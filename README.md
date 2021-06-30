# Sequences plugin

The Sequences plugin provides transactional sequences for GraphDB. A sequence is a long counter that can beb incremented atomically in a transaction to provide incremental IDs.

Additional documentation on the Plugin API and the project itself you can find
[here](http://graphdb.ontotext.com/free/plug-in-api.html) 

## Deployment

Below you can find the deployment steps for this plugin:

1. Build the project using `mvn clean package`
2. Unzip `./target/sequences-plugin-graphdb-plugin.zip` in  `<GDB_INST_DIR>/lib/plugins/`.

Once you start GraphDB and a repository is initialized you will see the following entries in the log:
```
Registering plugin sequences
Initializing plugin 'sequences'
```

## Usage

The plugin supports multiple concurrent sequences where each sequence is identified by an IRI chosen by the user. 

### Creating a sequence

Choose an IRI for your sequence, for example `http://example.com/my/seq1`. Insert the following triple to create a sequence whose next value will be 1:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

INSERT DATA {
    my:seq1 seq:create []
}
```

You can also create a sequence by providing the starting value, for example to create a sequence whose next value will be 10:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

INSERT DATA {
    my:seq1 seq:create 10
}
```

### Using a sequence

---
**NOTE**

Using the below examples will not work inside the GraphDB Workbench as they require a single transaction.

---

In order to use any sequence you must first **start a transaction** and then prepare the sequences for use by executing this update:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 

INSERT DATA {
    [] seq:prepare []
}
```

Then you can request new values from any sequence by running a query like this (for the sequence `http://example.com/my/seq1`):

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

SELECT ?next {
    my:seq1 seq:nextValue ?next
}
```

To query the last new value without incrementing the counter you can use a query like this:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

SELECT ?current {
    my:seq1 seq:currentValue ?current
}
```

Use the obtained values to construct IRIs, assign IDs or any other use case.

#### Advanced example

Prepares the sequences for use and inserts some new data using the sequence `http://example.com/my/seq1` where the subject of the newly inserted data is created from a value obtained from the sequence.

This example will work in the GraphDB Workbench as it performs everything in a single transaction (by separating individual operations using a semicolon).

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

# Prepares sequences for use
INSERT DATA {
    [] seq:prepare []
};

# Obtains a new value from the sequence and creates an IRI based on it,
# then inserts new triples using that IRI
INSERT {
    ?subject rdfs:label "This is my new document" ;
            a my:Type1
} WHERE {
    my:seq1 seq:nextValue ?next
    BIND(IRI(CONCAT("http://example.com/my-data/test", STR(?next))) as ?subject)    
};

# Retrieves the last obtained value, recreates the same IRI,
# and adds more data using the same IRI
INSERT {
    ?subject rdfs:comment ?comment ;
} WHERE {
    my:seq1 seq:currentValue ?current
    BIND(IRI(CONCAT("http://example.com/my-data/test", STR(?current))) as ?subject)
    BIND(CONCAT("The document ID is ", STR(?current)) as ?comment)    
}

```

Finally, **commit the transaction**.

### Dropping a sequence

Dropping a sequence is similar to creating it, for example to drop the sequence `http://example.com/my/seq1` execute this:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

INSERT DATA {
    my:seq1 seq:drop []
}
```


### Resetting a sequence

In some cases you might want to reset an existing sequences such that its next value will be a different number. Resetting is equivalent to dropping and recreating the sequence.

To reset a sequence such that its next value will be 1 execute this update:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

INSERT DATA {
    my:seq1 seq:reset []
}
```

You can also reset a sequence by providing the starting value, for example to reset a sequence such that its next value will be 10:

```sparql
PREFIX seq: <http://www.ontotext.com/plugins/sequences#> 
PREFIX my: <http://example.com/my/>

INSERT DATA {
    my:seq1 seq:reset 10
}
```


