package com.scaleunlimited.cascading.scheme.local;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.xml.sax.SAXException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.local.DirectoryFileOutputStream;

@SuppressWarnings("serial")
public class SolrScheme extends Scheme<Properties, InputStream, OutputStream, Void, SolrCollector> {

    public static final int DEFAULT_DEFAULT_MAX_SEGMENTS = 1;
    public static final String DEFAULT_DATA_DIR_PROPERTY_NAME = "solr.data.dir";
    
    private File _solrHomeDir;
    private int _maxSegments;
    private String _dataDirPropertyName;
    
    public SolrScheme(Fields schemeFields, String solrHomeDir) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrHomeDir, DEFAULT_DEFAULT_MAX_SEGMENTS);
    }
    
    public SolrScheme(Fields schemeFields, String solrHomeDir, int maxSegments) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrHomeDir, DEFAULT_DEFAULT_MAX_SEGMENTS, DEFAULT_DATA_DIR_PROPERTY_NAME);
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
        System.setProperty("enable.special.handlers", "false"); // All we need is the update request handler
        
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
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @Override
    public boolean source(FlowProcess<Properties> conf, SourceCall<Void, InputStream> sourceCall) throws IOException {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @Override
    public void sinkPrepare(FlowProcess<Properties> flowProcess, SinkCall<SolrCollector, OutputStream> sinkCall) throws IOException {
        if (!(sinkCall.getOutput() instanceof DirectoryFileOutputStream)) {
            throw new TapException("SolrScheme can only be used with a DirectoryTap in local mode");
        }
        
        DirectoryFileOutputStream os = (DirectoryFileOutputStream)sinkCall.getOutput();
        String path = os.asDirectory();

        // Set context to be the embedded solr server (or rather a wrapper for it, that handles caching)
        // TODO this call gets made BEFORE sinkConfInit, so I don't have the _dataDir set up at this point, which seems wrong.
        SolrCollector collector = new SolrCollector(flowProcess, getSinkFields(), _solrHomeDir.getAbsolutePath(), _maxSegments, _dataDirPropertyName, path);
        sinkCall.setContext(collector);
    }
    
    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
        // TODO What would I want to do here, if anything?
    }
    
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<SolrCollector, OutputStream> sinkCall) throws IOException {
        sinkCall.getContext().collect(sinkCall.getOutgoingEntry().getTuple());
    }
    
    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess, SinkCall<SolrCollector, OutputStream> sinkCall) throws IOException {
        SolrCollector collector = sinkCall.getContext();
        collector.cleanup();
    }

}
