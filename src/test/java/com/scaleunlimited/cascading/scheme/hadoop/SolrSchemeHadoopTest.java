package com.scaleunlimited.cascading.scheme.hadoop;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.hadoop.BytesSerialization;
import cascading.tuple.hadoop.TupleSerializationProps;

import com.scaleunlimited.cascading.scheme.core.AbstractSolrSchemeTest;

public class SolrSchemeHadoopTest extends AbstractSolrSchemeTest {

    private static final String TEST_DIR = "build/test/SolrSchemeHadoopTest/";

    @Override
    protected String getTestDir() {
        return TEST_DIR;
    }
    
    @Override
    protected FlowConnector makeFlowConnector() {
        Map<Object, Object> props = new HashMap<Object, Object>();
        TupleSerializationProps.addSerialization(props, BytesSerialization.class.getName());
        return new HadoopFlowConnector(props);
    }
    
    @Override
    protected FlowProcess<?> makeFlowProcess() {
        Map<Object, Object> props = new HashMap<Object, Object>();
        TupleSerializationProps.addSerialization(props, BytesSerialization.class.getName());
        return new HadoopFlowProcess(HadoopUtil.createJobConf(props, null));
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrConfDir) throws Exception {
        return new SolrScheme(schemeFields, solrConfDir);
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrConfDir, int maxSegments) throws Exception {
        return new SolrScheme(schemeFields, solrConfDir, maxSegments);
    }
    
    @Override
    protected Scheme<?, ?, ?, ?, ?> makeScheme(Fields schemeFields, String solrConfDir, int maxSegments, String dataDirPropertyName) throws Exception {
        return new SolrScheme(schemeFields, solrConfDir, maxSegments, dataDirPropertyName);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected Tap<?, ?, ?> makeSolrSink(Fields fields, String path) throws Exception {
        Scheme scheme = new SolrScheme(fields, SOLR_CONF_DIR);
        return new Hfs(scheme, path, SinkMode.REPLACE);
    }
    
    @Override
    protected Tap<?, ?, ?> makeSourceTap(Fields fields, String path) {
        return new Hfs(new SequenceFile(fields), path, SinkMode.REPLACE);
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
