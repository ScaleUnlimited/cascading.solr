cascading.solr
==============

This is a Cascading scheme for Solr.

It lets you easily add a Tap to a worfklow that generates a Lucene
index using Solr. The resulting index will have N shards for N reducers,
thus you can call the scheme's setNumSinkParts to control this value.

Indexes are built locally on the slave's hard disk drives, by leveraging
embedded Solr. Once the index has been built (and optionally optimized),
it is copied to the target location (HDFS, S3, etc) as specified by the
Tap. This improves the performance of building indexes, especially if
you can deploy multiple shards and thus build using many reducers.

