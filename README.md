Sofa River Plugin for ElasticSearch
==================================

The Sofa River plugin allows indexing of many CouchDB databases into an ElasticSearch index.

Motivation
----------

The Sofa river takes a different strategy from the CouchDB river. You specify a regular expression of database names `db_filter` in the configuration and all documents in matching databases are indexed. This means new databases will be automatically indexed as created without manually creating a new river.

Implementation
--------------

The Sofa river polls the CouchDB `_all_dbs` endpoint and then loops through all the databases. This is not an efficient way to monitor changes. Because of this the river will exponentially back off if no changes are detected. You can use the `backoff_min` and `backoff_max` parameters to configure the balance between resource intensivity and index latency.

Installation
------------

In order to install the plugin, simply run: `bin/plugin -install adamlofts/elasticsearch-river-sofa/0.0.5`.

Configuration
-------------

The following will index all CouchDB databases prefixed with `test` into the `my_es_index` index with doc type `my_es_type`.

    curl -XPUT 'localhost:9200/_river/river1/_meta' -d '{
        "type" : "sofa",
        "couchdb" : {
            "host" : "localhost",
            "port" : 5984,
            "db_filter" : "test.*"
        },
        "index" : {
            "index" : "index1",
            "type" : "test1",
    
            "bulk_size": 10,
            "backoff_min" : 1000,
            "backoff_max" : 60000
        }
    }'

Development
-------------

To build the plugin from source type `mvn assembly:assembly`.




