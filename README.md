cascading.solr
==============

This is a [Cascading](http://www.cascading.org/) [scheme](http://docs.cascading.org/cascading/2.1/userguide/html/ch03s05.html#N20867) for [Solr](http://lucene.apache.org/solr/).

It lets you easily add a Tap to a worfklow that generates a Lucene
index using Solr. The resulting index will have N shards for N reducers,
thus you can call the scheme's setNumSinkParts to control this value.

Indexes are built locally on the slave's hard disk drives, by leveraging
embedded Solr. Once the index has been built (and optionally optimized),
it is copied to the target location (HDFS, S3, etc) as specified by the
Tap. This improves the performance of building indexes, especially if
you can deploy multiple shards and thus build using many reducers.

You can find the latest version of the jar at [Conjars](http://conjars.org/search?q=cascading.solr)

To add this jar to your project (via Maven), include the following in your pom.xml:

```xml
	<repositories>
		<repository>
			<id>Conjars</id>
			<name>Cascading repository</name>
			<url>http://conjars.org/repo/</url>
		</repository>
	</repositories>
  ...
	<dependencies>
		<dependency>
			<groupId>com.scaleunlimited</groupId>
			<artifactId>cascading.solr</artifactId>
			<version>${cascading.solr.version}</version>
		</dependency>
		...
	</dependencies>
```
