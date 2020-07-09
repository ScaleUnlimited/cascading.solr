package com.scaleunlimited.cascading.scheme.hadoop;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.xml.sax.SAXException;

import com.scaleunlimited.cascading.scheme.core.SolrSchemeUtil;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;

@SuppressWarnings("serial")
public class SolrScheme extends Scheme<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>, Object[], Void> {
    
    private File _solrConfDir;
    private int _maxSegments;
    private boolean _isIncludeMetadata;
   
    public SolrScheme(Fields schemeFields, String solrConfDir) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrConfDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS);
    }
    
    public SolrScheme(Fields schemeFields, String solrConfDir, int maxSegments) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrConfDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS, false);
    }
    
    public SolrScheme(Fields schemeFields, String solrConfDir, boolean isIncludeMetadata) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrConfDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS, isIncludeMetadata);
    }
    
    public SolrScheme(Fields schemeFields, String solrConfDir, int maxSegments, boolean isIncludeMetadata) throws IOException, ParserConfigurationException, SAXException {
        super(schemeFields, schemeFields);

        _solrConfDir = new File(solrConfDir);
        _maxSegments = maxSegments;
        _isIncludeMetadata = isIncludeMetadata;

        SolrSchemeUtil.validate(_solrConfDir, schemeFields);
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
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        // Pick temp location in HDFS for conf files.
        // TODO KKr - should I get rid of this temp directory when we're done?
        String confDirname = _solrConfDir.getName();
        Path hdfsSolrConfDir = new Path(Hfs.getTempPath(conf),  "solr-conf-" + Util.createUniqueID() + "/" + confDirname);
        
        // Copy Solr conf directory into HDFS.
        try {
            FileSystem fs = hdfsSolrConfDir.getFileSystem(conf);
            fs.copyFromLocalFile(new Path(_solrConfDir.getAbsolutePath()), hdfsSolrConfDir);
        } catch (IOException e) {
            throw new TapException("Can't copy Solr conf directory into HDFS", e);
        }

        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(SolrOutputFormat.class);

        try {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, HadoopUtil.serializeBase64(getSinkFields(), conf));
        } catch (IOException e) {
            throw new TapException("Can't serialize sink fields", e);
        }

        conf.set(SolrOutputFormat.SOLR_CONF_PATH_KEY, hdfsSolrConfDir.toString());
        conf.setInt(SolrOutputFormat.MAX_SEGMENTS_KEY, _maxSegments);
        conf.setBoolean(SolrOutputFormat.INCLUDE_METADATA_KEY, _isIncludeMetadata);
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