package com.scaleunlimited.cascading.scheme.local;

import org.junit.Test;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;
import com.scaleunlimited.cascading.scheme.core.AbstractSolrSchemeTest;

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
        return new DirectoryTap(new SolrScheme(fields, SOLR_CORE_DIR), path);
    }
    
    @Override
    protected FlowConnector makeFlowConnector() {
        return new LocalFlowConnector();
    }
    
    @Override
    protected cascading.scheme.Scheme<?,?,?,?,?> makeScheme(Fields schemeFields, String solrCoreDir) throws Exception {
        return new SolrScheme(schemeFields, solrCoreDir);
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir, int maxSegments) throws Exception {
        return new SolrScheme(schemeFields, solrCoreDir, maxSegments);
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrCoreDir, int maxSegments, String dataDirPropertyName) throws Exception {
        return new SolrScheme(schemeFields, solrCoreDir, maxSegments, dataDirPropertyName);
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
    
}
