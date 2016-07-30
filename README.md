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

An example of using this SolrScheme (taken from `SolrSchemeHadoopTest.java` and `AbstractSolrSchemeTest.java`) looks like:

```java
    private static final String TEST_DIR = "build/test/SolrSchemeHadoopTest/";
    private static final String SOLR_HOME_DIR = "src/test/resources/solr-home-5.5/"; 
    protected static final String SOLR_CORE_DIR = SOLR_HOME_DIR + "collection1"; 

    @Test
    public void testSimpleIndexing() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "cat", "inStock", "image");

        final String in = TEST_DIR + "testSimpleIndexing/in";
        final String out = TEST_DIR + "testSimpleIndexing/out";

        byte[] imageData = new byte[] {0, 1, 2, 3, 5};
        
        // Create some data
        Tap source = new Hfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = source.openForWrite(makeFlowProcess());
        Tuple t = new Tuple();
        t.add(1);
        t.add("TurboWriter 2.3");
        t.add(395.50f);
        t.add(new Tuple("wordprocessor", "Japanese"));
        t.add(true);
        t.add(imageData);
        write.add(t);
        
        t = new Tuple();
        t.add(2);
        t.add("Shasta 1.0");
        t.add(95.00f);
        t.add("Chinese");
        t.add(false);
        
        BytesWritable bw = new BytesWritable(imageData);
        bw.setCapacity(imageData.length + 10);
        t.add(bw);
        write.add(t);
        write.close();

        // Now read from the results, and write to a Solr index.
        Pipe writePipe = new Pipe("tuples to Solr");

        Scheme scheme = new SolrScheme(testFields, SOLR_CORE_DIR);
        Tap solrSink = new Hfs(scheme, out, SinkMode.REPLACE);
        Flow flow = makeFlowConnector().connect(source, solrSink, writePipe);
        flow.complete();

        // Open up the Solr index, and do some searches.
        System.setProperty("solr.data.dir", out + "/part-00000");

        CoreContainer coreContainer = new CoreContainer(SOLR_HOME_DIR);
        coreContainer.load();
        SolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.Q, "turbowriter");

        QueryResponse res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        byte[] storedImageData = (byte[])res.getResults().get(0).getFieldValue("image");
        assertEquals(imageData, storedImageData);
        
        params.set(CommonParams.Q, "cat:Japanese");
        res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        
        params.set(CommonParams.Q, "cat:Chinese");
        res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        storedImageData = (byte[])res.getResults().get(0).getFieldValue("image");
        assertEquals(imageData, storedImageData);
        
        params.set(CommonParams.Q, "bogus");
        res = solrServer.query(params);
        assertEquals(0, res.getResults().size());
    }
```
