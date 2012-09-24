package com.scaleunlimited.cascading.scheme.hadoop;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class SolrOutputFormat extends FileOutputFormat<Tuple, Tuple> {
    private static final Logger LOGGER = Logger.getLogger(SolrOutputFormat.class);
    
    public static final String SOLR_HOME_PATH_KEY = "com.scaleunlimited.cascading.solr.homePath";
    public static final String SINK_FIELDS_KEY = "com.scaleunlimited.cascading.solr.sinkFields";
    public static final String MAX_SEGMENTS_KEY = "com.scaleunlimited.cascading.solr.maxSegments";
    public static final String DATA_DIR_PROPERTY_NAME_KEY = "com.scaleunlimited.cascading.solr.dataDirPropertyName";

    public static final int DEFAULT_MAX_SEGMENTS = 10;

    private static class SolrRecordWriter implements RecordWriter<Tuple, Tuple> {

        // TODO KKr - make this configurable.
        private static final int MAX_DOCS_PER_ADD = 500;
        
        private Path _outputPath;
        private FileSystem _outputFS;
        private Progressable _progress;
        
        private transient File _localIndexDir;
        private transient CoreContainer _coreContainer;
        private transient SolrServer _solrServer;
        private transient Fields _sinkFields;
        private transient int _maxSegments;
        private transient List<SolrInputDocument> _inputDocs;
        
        public SolrRecordWriter(JobConf conf, String name, Progressable progress) throws IOException {
            _progress = progress;
            
            // String tmpFolder = conf.getJobLocalDir();
            String tmpDir = System.getProperty("java.io.tmpdir");
            File localSolrHome = new File(tmpDir, "cascading.solr-" + UUID.randomUUID());
            
            // Copy solr home from HDFS to temp local location.
            Path sourcePath = new Path(conf.get(SOLR_HOME_PATH_KEY));
            FileSystem sourceFS = sourcePath.getFileSystem(conf);
            sourceFS.copyToLocalFile(sourcePath, new Path(localSolrHome.getAbsolutePath()));
            
            // Figure out where ultimately the results need to wind up.
            _outputPath = new Path(FileOutputFormat.getTaskOutputPath(conf, name), "index");
            _outputFS = _outputPath.getFileSystem(conf);

            // Get the set of fields we're indexing.
            _sinkFields = HadoopUtil.deserializeBase64(conf.get(SINK_FIELDS_KEY), conf, Fields.class);
            
            _maxSegments = conf.getInt(MAX_SEGMENTS_KEY, DEFAULT_MAX_SEGMENTS);
            
            // This is where data will wind up, inside of an index subdir.
            _localIndexDir = new File(localSolrHome, "data");

            _inputDocs = new ArrayList<SolrInputDocument>(MAX_DOCS_PER_ADD);
            
            // Fire up an embedded Solr server
            try {
                System.setProperty("solr.solr.home", localSolrHome.getAbsolutePath());
                System.setProperty(conf.get(DATA_DIR_PROPERTY_NAME_KEY), _localIndexDir.getAbsolutePath());
                
                CoreContainer.Initializer initializer = new CoreContainer.Initializer();
                _coreContainer = initializer.initialize();
                _solrServer = new EmbeddedSolrServer(_coreContainer, "");
            } catch (Exception e) {
                if (_coreContainer != null) {
                    _coreContainer.shutdown();
                }
                
                throw new IOException(e);
            }

        }
        
        @Override
        protected void finalize() throws Throwable {
            if (_solrServer != null) {
                _coreContainer.shutdown();
                _solrServer = null;
            }
            
            super.finalize();
        }
        
        @Override
        public void close(final Reporter reporter) throws IOException {

            try {
                flushInputDocuments(true);
                _coreContainer.shutdown();
                _solrServer = null;
            } catch (SolrServerException e) {
                throw new IOException(e);
            }
            
            // Finally we can copy the resulting index up to the target location in HDFS
            copyToHDFS();
        }

        private void copyToHDFS() throws IOException {
            File indexDir = new File(_localIndexDir, "index");
            
            // HACK!!! Hadoop has a bug where a .crc file locally with the matching name will
            // trigger an error, so we want to get rid of all such .crc files from inside of
            // the index dir.
            removeCrcFiles(indexDir);
            
            // Because we never write anything out, we need to tell Hadoop we're not hung.
            Thread reporterThread = startProgressThread();

            try {
                long indexSize = FileUtils.sizeOfDirectory(indexDir);
                LOGGER.info(String.format("Copying %d bytes of index from %s to %s", indexSize, _localIndexDir, _outputPath));
                _outputFS.copyFromLocalFile(true, new Path(indexDir.getAbsolutePath()), _outputPath);
            } finally {
                reporterThread.interrupt();
            }
        }
        
        private void removeCrcFiles(File dir) {
            File[] crcFiles = dir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".crc");
                }
            });
            
            for (File crcFile : crcFiles) {
                crcFile.delete();
            }
        }
        
        @Override
        public void write(Tuple key, Tuple value) throws IOException {
            SolrInputDocument doc = new SolrInputDocument();
            
            for (int i = 0; i < _sinkFields.size(); i++) {
                String name = (String)_sinkFields.get(i);
                Object fieldValue = value.getObject(i);
                if (fieldValue == null) {
                    // Don't add null values.
                } else if (fieldValue instanceof Tuple) {
                    Tuple list = (Tuple)fieldValue;
                    for (int j = 0; j < list.size(); j++) {
                        safeAdd(doc, name, list.getObject(j).toString());
                    }
                } else {
                    safeAdd(doc, name, fieldValue.toString());
                }
            }

            try {
                _inputDocs.add(doc);
                flushInputDocuments(false);
            } catch (SolrServerException e) {
                throw new IOException(e);
            }
        }
        
        private void safeAdd(SolrInputDocument doc, String fieldName, String value) {
            if ((value != null) && (value.length() > 0)) {
                doc.addField(fieldName, value);
            }
        }
        
        private void flushInputDocuments(boolean force) throws SolrServerException, IOException {
            if ((force && (_inputDocs.size() > 0)) || (_inputDocs.size() >= MAX_DOCS_PER_ADD)) {
                
                // Because we never write anything out, we need to tell Hadoop we're not hung.
                Thread reporterThread = startProgressThread();

                try {
                    UpdateRequest req = new UpdateRequest();
                    req.add(_inputDocs);
                    
                    // Set up overwite=false. See https://issues.apache.org/jira/browse/SOLR-653
                    // for details why we have to do it this way.
                    req.setParam(UpdateParams.OVERWRITE, Boolean.toString(false));
                    UpdateResponse rsp = req.process(_solrServer);
                    
                    // TODO KKr - figure out if we need to check this or not.
                    if (rsp.getStatus() != 0) {
                        throw new SolrServerException("Non-zero response from Solr: " + rsp.getStatus());
                    }
                    
                    if (force) {
                        _solrServer.commit(true, true);
                        _solrServer.optimize(true, true, _maxSegments);
                    }
                } catch (SolrServerException e) {
                    throw new IOException(e);
                } finally {
                    _inputDocs.clear();
                    reporterThread.interrupt();
                }
            }
        
        }
        
        /**
         * Fire off a thread that repeatedly calls Hadoop to tell it we're making progress.
         * @return
         */
        private Thread startProgressThread() {
            Thread result = new Thread() {
                @Override
                public void run() {
                    while (!isInterrupted()) {
                        _progress.progress();
                        
                        try {
                            sleep(10 * 1000);
                        } catch (InterruptedException e) {
                            interrupt();
                        }
                    }
                }
            };
            
            result.start();
            return result;
        }
        
    }
    
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        // TODO anything to do here?
    }

    @Override
    public RecordWriter<Tuple, Tuple> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        return new SolrRecordWriter(job, name, progress);
    }

}
