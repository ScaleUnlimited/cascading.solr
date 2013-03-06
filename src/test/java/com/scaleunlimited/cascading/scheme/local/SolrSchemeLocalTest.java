package com.scaleunlimited.cascading.scheme.local;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;

public class SolrSchemeLocalTest extends Assert {

    private static final String TEST_DIR = "build/test/SolrSchemeLocalTest/";
    private static final String SOLR_HOME_DIR = "src/test/resources/solr-home-4.1/"; 
    private static final String SOLR_CORE_DIR = SOLR_HOME_DIR + "collection1"; 
    
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
            new SolrScheme(new Fields("id", "bogus-field"), SOLR_CORE_DIR);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name doesn't exist"));
        }
    }
    
    @Test
    public void testSchemeMissingRequiredField() throws Exception {
        try {
            new SolrScheme(new Fields("sku"), SOLR_CORE_DIR);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name for required"));
        }
    }
    
    @Test
    public void testIndexSink() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "inStock");
        String out = TEST_DIR + "testIndexSink/out";

        DirectoryTap solrSink = new DirectoryTap(new SolrScheme(testFields, SOLR_CORE_DIR), out, SinkMode.REPLACE);
        
        TupleEntryCollector writer = solrSink.openForWrite(new LocalFlowProcess());

        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple(i, "product #" + i, i * 1.0f, true));
        }

        writer.close();
    }

    @Test
    public void testSimpleIndexing() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "cat", "inStock");

        final String in = TEST_DIR + "testSimpleIndexing/in";
        final String out = TEST_DIR + "testSimpleIndexing/out";

        FileTap localSource = new FileTap(new KryoScheme(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = localSource.openForWrite(new LocalFlowProcess());
        Tuple t = new Tuple();
        t.add(1);
        t.add("TurboWriter 2.3");
        t.add(395.50f);
        t.add(new Tuple("wordprocessor", "Japanese"));
        t.add(true);
        write.add(t);
        
        t = new Tuple();
        t.add(2);
        t.add("Shasta 1.0");
        t.add(95.00f);
        t.add("Chinese");
        t.add(false);
        write.add(t);
        write.close();

        // Now read from the results, and write to a Solr index.
        Pipe writePipe = new Pipe("tuples to Solr");

        DirectoryTap solrSink = new DirectoryTap(new SolrScheme(testFields, SOLR_CORE_DIR), out);
        Flow flow = new LocalFlowConnector().connect(localSource, solrSink, writePipe);
        flow.complete();

        // Open up the Solr index, and do some searches.
        System.setProperty("solr.solr.home", SOLR_HOME_DIR);
        System.setProperty("solr.data.dir", out + "/part-00000");
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer;
        coreContainer = initializer.initialize();
        SolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.Q, "turbowriter");

        QueryResponse res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        
        params.set(CommonParams.Q, "cat:Japanese");
        res = solrServer.query(params);
        assertEquals(1, res.getResults().size());
        
        params.set(CommonParams.Q, "bogus");
        res = solrServer.query(params);
        assertEquals(0, res.getResults().size());
    }
    
}
