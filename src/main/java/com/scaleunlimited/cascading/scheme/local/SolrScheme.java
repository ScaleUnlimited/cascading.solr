package com.scaleunlimited.cascading.scheme.local;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.scaleunlimited.cascading.local.DirectoryFileOutputStream;
import com.scaleunlimited.cascading.scheme.core.Metadata;
import com.scaleunlimited.cascading.scheme.core.SolrSchemeUtil;
import com.scaleunlimited.cascading.scheme.hadoop.SolrOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class SolrScheme extends Scheme<Properties, InputStream, OutputStream, Void, SolrCollector> {

    public static final int DEFAULT_DEFAULT_MAX_SEGMENTS = 1;
    
    private boolean _isIncludeMetadata;
    private String _dataDirPropertyName;
    private int _maxSegments;
    private File _solrCoreDir;
    private File _partDir;
    
    public SolrScheme(Fields schemeFields, String solrCoreDir) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrCoreDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS, false, SolrSchemeUtil.DEFAULT_DATA_DIR_PROPERTY_NAME);
    }
    
    public SolrScheme(Fields schemeFields, String solrCoreDir, int maxSegments, boolean isIncludeMetadata, String dataDirPropertyName) throws IOException, ParserConfigurationException, SAXException {
        super(schemeFields, schemeFields);

        _solrCoreDir = new File(solrCoreDir);
        _maxSegments = maxSegments;
        _isIncludeMetadata = isIncludeMetadata;
        _dataDirPropertyName = dataDirPropertyName;

        SolrSchemeUtil.validate(_solrCoreDir, _dataDirPropertyName, schemeFields);
    }
    
    @Override
    public boolean isSink() {
        return true;
    }
    
    @Override
    public boolean isSource() {
        return false;
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
        
        // Find the part-00000 directory to use as the Solr data directory,
        // and save it in case we need to write metadata there.
        DirectoryFileOutputStream os = (DirectoryFileOutputStream)sinkCall.getOutput();
        String path = os.asDirectory();
        _partDir = new File(path);

        // Set context to be the embedded solr server (or rather a wrapper for it, that handles caching)
        SolrCollector collector = new SolrCollector(flowProcess, getSinkFields(), _solrCoreDir, _maxSegments, _dataDirPropertyName, path);
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
        if (_isIncludeMetadata) {
            Metadata.writeMetadata(_partDir);
        }
    }

}
