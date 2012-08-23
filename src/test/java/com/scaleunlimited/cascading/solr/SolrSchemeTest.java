package com.scaleunlimited.cascading.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class SolrSchemeTest {

    private static final String TEST_DIR = "build/test/SolrSchemeTest/";
    private static final String SOLR_HOME_NUTCH = "src/test/resources/solr-home-nutch/"; 
    private static final String SOLR_HOME_31 = "src/test/resources/solr-home-3.1/"; 
    
    @Before
    public void setup() throws IOException {
        File outputDir = new File(TEST_DIR);
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    @Test
    public void testSchemeChecksMissingConf() throws Exception {
        try {
            new SolrScheme(new Fields("a", "b"), "bogus-directory");
            fail("Should have thrown exception");
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testSchemeChecksBadConf() throws Exception {
        try {
            new SolrScheme(new Fields("a", "b"), "src/test/resources");
            fail("Should have thrown exception");
        } catch (TapException e) {
        }
    }
    
    @Test
    public void testSchemeWrongFields() throws Exception {
        try {
            // Need to make sure we include the required fields.
            new SolrScheme(new Fields("id", "url", "bogus-field"), SOLR_HOME_NUTCH);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name doesn't exist"));
        }
    }
    
    @Test
    public void testSchemeMissingRequiredField() throws Exception {
        try {
            new SolrScheme(new Fields("host"), SOLR_HOME_NUTCH);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name for required"));
        }
    }
    
    @Test
    public void testIndexSink() throws Exception {
        final Fields testFields = new Fields("id", "host", "url", "title", "content", "type");
        String out = "build/test/SolrSchemeTest/testIndexSink/out";

        final String solrHome = SOLR_HOME_NUTCH;
        Lfs solrSink = new Lfs(new SolrScheme(testFields, solrHome), out, SinkMode.REPLACE);
        
        TupleEntryCollector writer = solrSink.openForWrite(new HadoopFlowProcess());

        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple("http://domain.com", "domain.com", "http://domain.com", "Title", "content", "html/text"));
        }

        writer.close();
    }

    @Test
    public void testSimpleIndexing() throws Exception {
        final Fields testFields = new Fields("id", "host", "url", "title", "content", "type");

        final String in = TEST_DIR + "testSimpleIndexing/in";
        final String out = TEST_DIR + "testSimpleIndexing/out";

        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());
        Tuple t = new Tuple();
        t.add("http://domain.com/page.html");
        t.add("domain.com");
        t.add("http://domain.com/page.html");
        t.add("Title");
        t.add("This is some content that I can use to search for words like Solr and BixoLabs");
        t.add(new Tuple("type1", "type2"));
        write.add(t);
        
        t = new Tuple();
        t.add("http://domain2.com/page.html");
        t.add("domain2.com");
        t.add("http://domain2.com/page.html");
        t.add("Super Title");
        t.add("Different stuff");
        t.add(new Tuple("type1"));
        write.add(t);
        write.close();

        // Now read from the results, and write to a Solr index.
        Pipe writePipe = new Pipe("tuples to Solr");

        final String solrHome = SOLR_HOME_NUTCH;
        Lfs solrSink = new Lfs(new SolrScheme(testFields, solrHome), out);
        Flow flow = new HadoopFlowConnector().connect(lfsSource, solrSink, writePipe);
        flow.complete();

        // Open up the Solr index, and do some searches.
        System.setProperty("solr.solr.home", solrHome);
        System.setProperty("solr.data.dir", out + "/part-00000");
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer;
        coreContainer = initializer.initialize();
        SolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.Q, "BixoLabs");

        QueryResponse res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        
        params.set(CommonParams.Q, "title:\"super title\"");
        res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        
        params.set(CommonParams.Q, "bogus");
        res = solrServer.query(params);
        assertEquals(0, res.getResults().size());
        
        params.set(CommonParams.Q, "type:type1");
        res = solrServer.query(params);
        assertEquals(2, res.getResults().size());
    }
    
    @Test
    public void testSolr31Indexing() throws Exception {
        final Fields testFields = new Fields("id", "name");

        final String in = TEST_DIR + "testSolr31Indexing/in";
        final String out = TEST_DIR + "testSolr31Indexing/out";

        Lfs lfsSource = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfsSource.openForWrite(new HadoopFlowProcess());
        Tuple t = new Tuple();
        t.add("1");
        t.add("name1");
        write.add(t);
        
        t = new Tuple();
        t.add("2");
        t.add("name2");
        write.add(t);
        write.close();

        // Now read from the results, and write to a Solr index.
        Pipe writePipe = new Pipe("tuples to Solr");

        final String solrHome = SOLR_HOME_31;
        Tap solrSink = new Lfs(new SolrScheme(testFields, solrHome), out);
        Flow flow = new HadoopFlowConnector().connect(lfsSource, solrSink, writePipe);
        flow.complete();

        // Open up the Solr index, and do some searches.
        System.setProperty("solr.solr.home", solrHome);
        System.setProperty("solr.data.dir", out + "/part-00000");
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer;
        coreContainer = initializer.initialize();
        SolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.Q, "name:name1");

        QueryResponse res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
    }
    

}
