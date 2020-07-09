package com.scaleunlimited.cascading.scheme.local;

import org.junit.Test;

import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;
import com.scaleunlimited.cascading.scheme.core.AbstractSolrSchemeTest;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class SolrSchemeLocalTest extends AbstractSolrSchemeTest {

    private static final String TEST_DIR = "build/test/SolrSchemeLocalTest/";
    
    @Override
    protected String getTestDir() {
        return TEST_DIR;
    }
    
    @Override
    protected FlowProcess<?> makeFlowProcess() {
        return new LocalFlowProcess();
    }
    
    @Override
    protected Tap<?, ?, ?> makeSourceTap(Fields fields, String path) {
        return new FileTap(new KryoScheme(fields), path, SinkMode.REPLACE);
    }
    
    @Override
    protected Tap<?, ?, ?> makeSolrSink(Fields fields, String path) throws Exception {
        return new DirectoryTap(new SolrScheme(fields, SOLR_CONF_DIR), path);
    }
    
    @Override
    protected Tap<?, ?, ?> makeSolrSink(Scheme scheme, String path) throws Exception {
        return new DirectoryTap(scheme, path);
    }
    
    @Override
    protected FlowConnector makeFlowConnector() {
        return new LocalFlowConnector();
    }
    
    @Override
    protected cascading.scheme.Scheme<?,?,?,?,?> makeScheme(Fields schemeFields, String solrConfDir) throws Exception {
        return new SolrScheme(schemeFields, solrConfDir);
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrConfDir, int maxSegments) throws Exception {
        return new SolrScheme(schemeFields, solrConfDir, maxSegments);
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrConfDir, boolean isIncludeMetadata) throws Exception {
        return new SolrScheme(schemeFields, solrConfDir, isIncludeMetadata);
    }
    
    @Test
    public void testSchemeChecksMissingConf() throws Exception {
        super.testSchemeChecksMissingConf();
    }
    
    @Test
    public void testSchemeChecksBadConf() throws Exception {
        super.testSchemeChecksBadConf();
    }
    
    @Test
    public void testSchemeWrongFields() throws Exception {
        super.testSchemeWrongFields();
    }
    
    @Test
    public void testSchemeMissingRequiredField() throws Exception {
        super.testSchemeMissingRequiredField();
    }
    
    @Test
    public void testIndexSink() throws Exception {
        super.testIndexSink();
    }

    @Test
    public void testSimpleIndexing() throws Exception {
        super.testSimpleIndexing();
    }
    
    @Test
    public void testMd5() throws Exception {
        super.testMd5();
    }
    
}
