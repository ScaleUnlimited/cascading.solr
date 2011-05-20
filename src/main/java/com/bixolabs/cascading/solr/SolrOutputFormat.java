package com.bixolabs.cascading.solr;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.xml.sax.SAXException;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;

public class SolrOutputFormat implements OutputFormat<Tuple, Tuple> {
    private static final Logger LOGGER = Logger.getLogger(SolrOutputFormat.class);
    
    public static final String SOLR_HOME_PATH_KEY = "com.bixolabs.cascading.solr.homePath";
    public static final String SINK_FIELDS_KEY = "com.bixolabs.cascading.solr.sinkFields";

    private static class SolrRecordWriter implements RecordWriter<Tuple, Tuple> {

        private Path _outputPath;
        private FileSystem _outputFS;
        private transient File _localIndexDir;
        private transient SolrServer _solrServer;
        private transient Fields _sinkFields;

        public SolrRecordWriter(JobConf conf, String name) throws IOException {
            String tmpFolder = System.getProperty("java.io.tmpdir");
            File localSolrHome = new File(tmpFolder, UUID.randomUUID().toString());

            // Copy solr home from HDFS to temp local location.
            Path sourcePath = new Path(conf.get(SOLR_HOME_PATH_KEY));
            FileSystem sourceFS = sourcePath.getFileSystem(conf);
            sourceFS.copyToLocalFile(sourcePath, new Path(localSolrHome.getAbsolutePath()));
            
            // Figure out where ultimately the results need to wind up.
            _outputPath = new Path(FileOutputFormat.getTaskOutputPath(conf, name), "index");
            _outputFS = _outputPath.getFileSystem(conf);

            // Get the set of fields we're indexing.
            _sinkFields = (Fields)Util.deserializeBase64(conf.get(SINK_FIELDS_KEY));
            
            // This is where data will wind up, inside of an index subdir.
            _localIndexDir = new File(localSolrHome, "data");

            // Fire up an embedded Solr server
            try {
                System.setProperty("solr.solr.home", localSolrHome.getAbsolutePath());
                System.setProperty("solr.data.dir", _localIndexDir.getAbsolutePath());
                CoreContainer.Initializer initializer = new CoreContainer.Initializer();
                CoreContainer coreContainer;
                coreContainer = initializer.initialize();
                _solrServer = new EmbeddedSolrServer(coreContainer, "");
            } catch (ParserConfigurationException e) {
                throw new IOException(e);
            } catch (SAXException e) {
                throw new IOException(e);
            }

        }
        
        @Override
        public void close(final Reporter reporter) throws IOException {
            
            // Hadoop need to know we still working on it.
            Thread reporterThread = new Thread() {
                @Override
                public void run() {
                    while (!isInterrupted()) {
                        reporter.progress();
                        try {
                            sleep(10 * 1000);
                        } catch (InterruptedException e) {
                            interrupt();
                        }
                    }
                }
            };
            reporterThread.start();

            try {
                _solrServer.commit();
                _solrServer.optimize();
                
                LOGGER.info("Copying index from local to " + _outputPath);
                File indexDir = new File(_localIndexDir, "index");
                _outputFS.copyFromLocalFile(true, new Path(indexDir.getAbsolutePath()), _outputPath);
            } catch (SolrServerException e) {
                throw new IOException(e);
            } finally {
                reporterThread.interrupt();
            }
        }

        @Override
        public void write(Tuple key, Tuple value) throws IOException {
            SolrInputDocument doc = new SolrInputDocument();
            
            for (int i = 0; i < _sinkFields.size(); i++) {
                String name = (String)_sinkFields.get(i);
                Comparable fieldValue = value.get(i);
                if (fieldValue instanceof Tuple) {
                    Tuple list = (Tuple)fieldValue;
                    for (int j = 0; j < list.size(); j++) {
                        doc.addField(name, list.getObject(j).toString());
                    }
                } else {
                    doc.addField(name, "" + fieldValue.toString());
                }
            }

            try {
                _solrServer.add(doc);
            } catch (SolrServerException e) {
                throw new IOException(e);
            }
        }
        
    }
    
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        // TODO anything to do here?
    }

    @Override
    public RecordWriter<Tuple, Tuple> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        return new SolrRecordWriter(job, name);
    }

}
