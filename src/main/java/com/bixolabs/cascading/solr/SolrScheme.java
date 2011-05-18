package com.bixolabs.cascading.solr;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.xml.sax.SAXException;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import cascading.util.Util;

@SuppressWarnings("serial")
public class SolrScheme extends Scheme {

    private File _solrConfDir;
    private Fields _schemeFields;
    
    public SolrScheme(Fields schemeFields, String solrConfDir) throws IOException, ParserConfigurationException, SAXException {
        // Verify solrConfDir exists
        _solrConfDir = new File(solrConfDir);
        if (!_solrConfDir.exists() || !_solrConfDir.isDirectory()) {
            throw new TapException("Solr conf directory doesn't exist: " + solrConfDir);
        }
        
        System.setProperty("solr.solr.home", _solrConfDir.getAbsolutePath());
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer = initializer.initialize();
        Collection<SolrCore> cores = coreContainer.getCores();
        if (cores.size() != 1) {
            throw new TapException("Solr config can only have one core");
        }
        
        IndexSchema schema = null;
        for (SolrCore core : cores) {
            schema = core.getSchema();
        }
        
        Map<String, SchemaField> solrFields = schema.getFields();
        Set<String> schemeFieldnames = new HashSet<String>();
        
        for (int i = 0; i < schemeFields.size(); i++) {
            String fieldName = schemeFields.get(i).toString();
            if (!solrFields.containsKey(fieldName)) {
                throw new TapException("Sink field name doesn't exist in Solr schema: " + fieldName);
            }
            schemeFieldnames.add(fieldName);
        }
        
        for (String solrFieldname : solrFields.keySet()) {
            SchemaField solrField = solrFields.get(solrFieldname);
            if (solrField.isRequired() && !schemeFieldnames.contains(solrFieldname)) {
                throw new TapException("No sink field name for required Solr field: " + solrFieldname);
            }
        }
        
        _schemeFields = schemeFields;
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        Tuple result = getSinkFields() != null ? tupleEntry.selectTuple(getSinkFields()) : tupleEntry.getTuple();
        outputCollector.collect(Tuples.NULL, result);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
        
        // Pick temp location in HDFS for conf files.
        // TODO KKr - do I need to worry about multiple sinks getting initialized,
        // and thus copying over each other?
        // TODO KKr - should I get rid of this temp directory when we're done?
        Path outputPath = FileOutputFormat.getOutputPath(conf);
        Path hdfsSolrConfDir = new Path(outputPath, "_temporary/solr-home");
        
        // Copy Solr conf into HDFS.
        FileSystem fs = hdfsSolrConfDir.getFileSystem(conf);
        fs.copyFromLocalFile(new Path(_solrConfDir.getAbsolutePath()), hdfsSolrConfDir);

        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(SolrOutputFormat.class);
        
        if (getSinkFields() == Fields.ALL) {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, Util.serializeBase64(_schemeFields));
        } else {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, Util.serializeBase64(getSinkFields()));
        }
        
        conf.set(SolrOutputFormat.SOLR_CONF_PATH_KEY, hdfsSolrConfDir.toString());
    }

    @Override
    public Tuple source(Object key, Object value) {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

}
