package com.scaleunlimited.cascading.solr;

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
import org.apache.hadoop.mapred.RecordReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.xml.sax.SAXException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class SolrScheme extends Scheme<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>, Object[], Void> {

    public static final String DEFAULT_DATA_DIR_PROPERTY_NAME = "solr.data.dir";
    
    private File _solrHomeDir;
    private int _maxSegments;
    private String _dataDirPropertyName;
    
    public SolrScheme(Fields schemeFields, String solrHomeDir) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrHomeDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS);
    }
    
    public SolrScheme(Fields schemeFields, String solrHomeDir, int maxSegments) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrHomeDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS, DEFAULT_DATA_DIR_PROPERTY_NAME);
    }
    
    public SolrScheme(Fields schemeFields, String solrHomeDir, int maxSegments, String dataDirPropertyName) throws IOException, ParserConfigurationException, SAXException {
        super(schemeFields, schemeFields);

        // Verify solrHomeDir exists
        _solrHomeDir = new File(solrHomeDir);
        if (!_solrHomeDir.exists() || !_solrHomeDir.isDirectory()) {
            throw new TapException("Solr home directory doesn't exist: " + solrHomeDir);
        }
        
        _maxSegments = maxSegments;
        _dataDirPropertyName = dataDirPropertyName;
        
        // Set up a temp location for data, so when we instantiate the coreContainer,
        // we don't pollute the solr home with a /data sub-dir.
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpDataDir = new File(tmpFolder, UUID.randomUUID().toString());
        tmpDataDir.mkdir();
        
        System.setProperty("solr.solr.home", _solrHomeDir.getAbsolutePath());
        System.setProperty(_dataDirPropertyName, tmpDataDir.getAbsolutePath());
        
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer = null;
        
        try {
            coreContainer = initializer.initialize();
            Collection<SolrCore> cores = coreContainer.getCores();
            if (cores.size() == 0) {
                throw new TapException("No Solr cores are available");
            } else if (cores.size() > 1) {
                throw new TapException("Solr config can only have one core");
            }

            IndexSchema schema = cores.iterator().next().getSchema();
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
        } finally {
            if (coreContainer != null) {
                coreContainer.shutdown();
            }
        }
    }
    
    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        // Pick temp location in HDFS for conf files.
        // TODO KKr - do I need to worry about multiple sinks getting initialized,
        // and thus copying over each other?
        // TODO KKr - should I get rid of this temp directory when we're done?
        Path outputPath = FileOutputFormat.getOutputPath(conf);
        Path hdfsSolrHomeDir = new Path(outputPath, "_tempsolr/solr-home");
        
        // Copy Solr conf into HDFS.
        try {
            FileSystem fs = hdfsSolrHomeDir.getFileSystem(conf);
            fs.copyFromLocalFile(new Path(_solrHomeDir.getAbsolutePath()), hdfsSolrHomeDir);
        } catch (IOException e) {
            throw new TapException("Can't copy Solr conf into HDFS", e);
        }

        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(SolrOutputFormat.class);

        try {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, HadoopUtil.serializeBase64(getSinkFields(), conf));
        } catch (IOException e) {
            throw new TapException("Can't serialize sink fields", e);
        }

        conf.set(SolrOutputFormat.SOLR_HOME_PATH_KEY, hdfsSolrHomeDir.toString());
        conf.setInt(SolrOutputFormat.MAX_SEGMENTS_KEY, _maxSegments);
        conf.set(SolrOutputFormat.DATA_DIR_PROPERTY_NAME_KEY, _dataDirPropertyName);
    }

    @Override
    public boolean source(FlowProcess<JobConf> conf, SourceCall<Object[], RecordReader<Tuple, Tuple>> sourceCall) throws IOException {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector<Tuple, Tuple>> sinkCall) throws IOException {
        sinkCall.getOutput().collect(Tuple.NULL, sinkCall.getOutgoingEntry().getTuple());
    }
}
