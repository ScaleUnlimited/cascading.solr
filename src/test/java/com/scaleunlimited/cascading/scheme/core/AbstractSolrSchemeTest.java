package com.scaleunlimited.cascading.scheme.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.scheme.local.SolrScheme;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public abstract class AbstractSolrSchemeTest extends Assert {

    private static final String SOLR_HOME_DIR = "src/test/resources/solr-home-4.1/"; 
    protected static final String SOLR_CORE_DIR = SOLR_HOME_DIR + "collection1"; 

    protected abstract String getTestDir();
    
    protected abstract Tap<?, ?, ?> makeSourceTap(Fields fields, String path);
    protected abstract FlowProcess<?> makeFlowProcess();
    protected abstract Tap<?, ?, ?> makeSolrSink(Scheme<?, ?, ?, ?, ?> scheme, String path) throws Exception;
    protected abstract Tap<?, ?, ?> makeSolrSink(Fields fields, String path) throws Exception;
    protected abstract FlowConnector makeFlowConnector();
    
    protected abstract Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir) throws Exception;
    
    protected abstract Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir, boolean isIncludeMetadata) throws Exception;
    
    @Before
    public void setup() throws IOException {
        File outputDir = new File(getTestDir());
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    protected void testSchemeChecksMissingConf() throws Exception {
        try {
            makeScheme(new Fields("a", "b"), "bogus-directory");
            fail("Should have thrown exception");
        } catch (Exception e) {
        }
    }

    protected void testSchemeChecksBadConf() throws Exception {
        try {
            makeScheme(new Fields("a", "b"), "src/test/resources");
            fail("Should have thrown exception");
        } catch (TapException e) {
        }
    }
    
    protected void testSchemeWrongFields() throws Exception {
        try {
            // Need to make sure we include the required fields.
            makeScheme(new Fields("id", "bogus-field"), SOLR_CORE_DIR);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name doesn't exist"));
        }
    }

    protected void testSchemeMissingRequiredField() throws Exception {
        try {
            makeScheme(new Fields("sku"), SOLR_CORE_DIR);
            fail("Should have thrown exception");
        } catch (TapException e) {
            assert(e.getMessage().contains("field name for required"));
        }
    }
    
    protected void testIndexSink() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "inStock");
        String out = getTestDir() + "testIndexSink/out";

        DirectoryTap solrSink = new DirectoryTap(new SolrScheme(testFields, SOLR_CORE_DIR), out, SinkMode.REPLACE);
        
        TupleEntryCollector writer = solrSink.openForWrite(new LocalFlowProcess());

        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple(i, "product #" + i, i * 1.0f, true));
        }

        writer.close();
    }
    
    protected void testSimpleIndexing() throws Exception {
        final Fields testFields = new Fields("id", "name", "price", "cat", "inStock", "image");

        final String in = getTestDir() + "testSimpleIndexing/in";
        final String out = getTestDir() + "testSimpleIndexing/out";

        byte[] imageData = new byte[] {0, 1, 2, 3, 5};
        
        Tap source = makeSourceTap(testFields, in);
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

        Tap solrSink = makeSolrSink(testFields, out);
        Flow flow = makeFlowConnector().connect(source, solrSink, writePipe);
        flow.complete();

        // Open up the Solr index, and do some searches.
        // TODO switch to new way of initializing Container
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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    protected void testMd5() throws Exception {
        
        // Write input data
        final Fields testFields = new Fields("id", "name", "price", "inStock");
        final File inDir = new File(getTestDir() + "testMd5/in");
        Tap source = makeSourceTap(testFields, inDir.getAbsolutePath());
        TupleEntryCollector writer = source.openForWrite(makeFlowProcess());
        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple(i, "product #" + i, i * 1.0f, true));
        }
        writer.close();

        // Read input data and then write it to Solr index
        final File outDir =  new File(getTestDir() + "testMd5/out");
        Scheme scheme = makeScheme(testFields, SOLR_CORE_DIR, true);
        Tap<?, ?, ?> solrSink = makeSolrSink(scheme, outDir.getPath());
        Pipe writePipe = new Pipe("tuples to Solr");
        Flow flow = makeFlowConnector().connect(source, solrSink, writePipe);
        flow.complete();
        
        // Check MD5s saved within each part directory
        File[] partDirs = outDir.listFiles(new FilenameFilter() {
            public boolean accept(File file, String string) {
                return string.startsWith("part-");
            }
        });
        for (File partDir : partDirs) {
            
            // Read MD5 metadata into a map
            File md5File = new File(partDir, Metadata.MD5_FILE_NAME);
            FileInputStream fis = new FileInputStream(md5File);
            List<String> lines = IOUtils.readLines(fis);
            Map<String, String> indexFileNameToMD5Map =
                new HashMap<String, String>();
            for (String rawLine : lines) {
                String line = rawLine.replaceFirst("#.*$", "").trim();
                if (!line.isEmpty()) {
                    String fields[] = line.split("\t", 3);
                    if (fields.length < 2) {
                        throw new RuntimeException(     "Invalid MD5 metadata (expected <file path>\t<MD5>):\n"
                                                    +   line);
                    }
                    String indexFileName = fields[0].trim();
                    String md5 = fields[1].trim();
                    assertNull(indexFileNameToMD5Map.put(indexFileName, md5));
                }
            }
            
            // Compare map to MD5 of index files in part directory
            File indexDir = new File(partDir, "index");
            File[] indexFiles = indexDir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return !(name.endsWith(".crc"));
                }
            });
            for (File indexFile : indexFiles) {
                String expectedMD5 = getMD5(indexFile);
                assertEquals(   "wrong MD5 for " + indexFile,
                                expectedMD5, 
                                indexFileNameToMD5Map.get(indexFile.getName()));
            }
        }
    }
    
    private static String getMD5(File indexFile) throws IOException {
        InputStream is = new FileInputStream(indexFile);
        String result = null;
        try {
            result = DigestUtils.md2Hex(is);
        } finally {
            is.close();
        }
        return result;
    }


    private static void assertEquals(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }
    

}
