package com.bixolabs.cascading.solr;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

    private File _solrHomeDir;
    private Fields _schemeFields;
    
    public SolrScheme(Fields schemeFields, String solrHomeDir) throws IOException, ParserConfigurationException, SAXException {
        // Verify solrHomeDir exists
        _solrHomeDir = new File(solrHomeDir);
        if (!_solrHomeDir.exists() || !_solrHomeDir.isDirectory()) {
            throw new TapException("Solr home directory doesn't exist: " + solrHomeDir);
        }
        
        // Set up a temp location for data, so when we instantiate the coreContainer,
        // we don't pollute the solr home with a /data sub-dir.
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpDataDir = new File(tmpFolder, UUID.randomUUID().toString());
        tmpDataDir.mkdir();
        
        System.setProperty("solr.solr.home", _solrHomeDir.getAbsolutePath());
        System.setProperty("solr.data.dir", tmpDataDir.getAbsolutePath());
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer = null;
        
        try {
            coreContainer = initializer.initialize();
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
        } finally {
            if (coreContainer != null) {
                coreContainer.shutdown();
            }
        }
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
        Path hdfsSolrHomeDir = new Path(outputPath, "_tempsolr/solr-home");
        
        // Copy Solr conf into HDFS.
        FileSystem fs = hdfsSolrHomeDir.getFileSystem(conf);
        fs.copyFromLocalFile(new Path(_solrHomeDir.getAbsolutePath()), hdfsSolrHomeDir);

        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(SolrOutputFormat.class);
        
        if (getSinkFields() == Fields.ALL) {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, Util.serializeBase64(_schemeFields));
        } else {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, Util.serializeBase64(getSinkFields()));
        }
        
        conf.set(SolrOutputFormat.SOLR_HOME_PATH_KEY, hdfsSolrHomeDir.toString());
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
