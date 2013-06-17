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

import com.scaleunlimited.cascading.scheme.core.SolrSchemeUtil;

@SuppressWarnings("serial")
public class SolrScheme extends Scheme<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>, Object[], Void> {

    private File _solrCoreDir;
    private int _maxSegments;
    private String _dataDirPropertyName;
    
    public SolrScheme(Fields schemeFields, String solrCoreDir) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrCoreDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS);
    }
    
    public SolrScheme(Fields schemeFields, String solrCoreDir, int maxSegments) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, solrCoreDir, SolrOutputFormat.DEFAULT_MAX_SEGMENTS, SolrSchemeUtil.DEFAULT_DATA_DIR_PROPERTY_NAME);
    }
    
    public SolrScheme(Fields schemeFields, String solrCoreDir, int maxSegments, String dataDirPropertyName) throws IOException, ParserConfigurationException, SAXException {
        super(schemeFields, schemeFields);

        _solrCoreDir = new File(solrCoreDir);
        _maxSegments = maxSegments;
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
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        throw new TapException("SolrScheme can only be used as a sink, not a source");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        // Pick temp location in HDFS for conf files.
        // TODO KKr - should I get rid of this temp directory when we're done?
        String coreDirname = _solrCoreDir.getName();
        Path hdfsSolrCoreDir = new Path(Hfs.getTempPath(conf),  "solr-core-" + Util.createUniqueID() + "/" + coreDirname);
        
        // Copy Solr core directory into HDFS.
        try {
            FileSystem fs = hdfsSolrCoreDir.getFileSystem(conf);
            fs.copyFromLocalFile(new Path(_solrCoreDir.getAbsolutePath()), hdfsSolrCoreDir);
        } catch (IOException e) {
            throw new TapException("Can't copy Solr core directory into HDFS", e);
        }

        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(SolrOutputFormat.class);

        try {
            conf.set(SolrOutputFormat.SINK_FIELDS_KEY, HadoopUtil.serializeBase64(getSinkFields(), conf));
        } catch (IOException e) {
            throw new TapException("Can't serialize sink fields", e);
        }

        conf.set(SolrOutputFormat.SOLR_CORE_PATH_KEY, hdfsSolrCoreDir.toString());
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
