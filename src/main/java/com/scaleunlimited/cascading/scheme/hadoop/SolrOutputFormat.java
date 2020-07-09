package com.scaleunlimited.cascading.scheme.hadoop;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.scheme.core.KeepAliveHook;
import com.scaleunlimited.cascading.scheme.core.Metadata;
import com.scaleunlimited.cascading.scheme.core.SolrSchemeUtil;
import com.scaleunlimited.cascading.scheme.core.SolrWriter;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class SolrOutputFormat extends FileOutputFormat<Tuple, Tuple> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrOutputFormat.class);
    
    public static final String SOLR_CONF_PATH_KEY = "com.scaleunlimited.cascading.solr.confPath";
    public static final String SINK_FIELDS_KEY = "com.scaleunlimited.cascading.solr.sinkFields";
    public static final String MAX_SEGMENTS_KEY = "com.scaleunlimited.cascading.solr.maxSegments";
    public static final String INCLUDE_METADATA_KEY = "com.scaleunlimited.cascading.solr.includeMetadata";

    public static final int DEFAULT_MAX_SEGMENTS = 10;

    private static class SolrRecordWriter implements RecordWriter<Tuple, Tuple> {

        private Path _outputPath;
        private FileSystem _outputFS;
        private boolean _isIncludeMetadata;
        
        private transient KeepAliveHook _keepAliveHook;
        private transient File _localIndexDir;
        private transient SolrWriter _solrWriter;
        
        public SolrRecordWriter(JobConf conf, String name, Progressable progress) throws IOException {
            
            // Copy Solr conf directory from HDFS to temp local location.
            Path sourcePath = new Path(conf.get(SOLR_CONF_PATH_KEY));
            String confName = sourcePath.getName();
            String tmpDir = System.getProperty("java.io.tmpdir");
            File localSolrConf = new File(tmpDir, "cascading.solr-" + UUID.randomUUID() + "/" + confName);
            FileSystem sourceFS = sourcePath.getFileSystem(conf);
            sourceFS.copyToLocalFile(sourcePath, new Path(localSolrConf.getAbsolutePath()));
            
            // Figure out where ultimately the results need to wind up.
            _outputPath = new Path(FileOutputFormat.getTaskOutputPath(conf, name), "index");
            _outputFS = _outputPath.getFileSystem(conf);

            // Get the set of fields we're indexing.
            Fields sinkFields = HadoopUtil.deserializeBase64(conf.get(SINK_FIELDS_KEY), conf, Fields.class);
            
            // Load optional configuration parameters.
            int maxSegments = conf.getInt(MAX_SEGMENTS_KEY, DEFAULT_MAX_SEGMENTS);
            _isIncludeMetadata = conf.getBoolean(INCLUDE_METADATA_KEY, false);
            
            // Set up local Solr home.
            File localSolrHome = SolrSchemeUtil.makeTempSolrHome(localSolrConf, null);

            // This is where data will wind up, inside of an index subdir.
            _localIndexDir = new File(localSolrHome, "data");

            _keepAliveHook = new HadoopKeepAliveHook(progress);
            
            _solrWriter = new SolrWriter(_keepAliveHook, sinkFields, _localIndexDir.getAbsolutePath(), localSolrConf, maxSegments) { };
        }
        
        @Override
        protected void finalize() throws Throwable {
            if (_solrWriter != null) {
                _solrWriter.cleanup();
                _solrWriter = null;
            }
            
            super.finalize();
        }
        
        @Override
        public void close(final Reporter reporter) throws IOException {
            _solrWriter.cleanup();
            
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
                if (_isIncludeMetadata) {
                    File localMetadataFile = Metadata.writeMetadata(_localIndexDir);
                    Path metadataPath = new Path(_outputPath.getParent(), Metadata.MD5_FILE_NAME);
                    LOGGER.info(String.format("Copying index metadata from %s to %s", _localIndexDir, metadataPath));
                    _outputFS.copyFromLocalFile(true, new Path(localMetadataFile.getAbsolutePath()), metadataPath);
                }

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
            _solrWriter.add(value);
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
                        _keepAliveHook.keepAlive();
                        
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
