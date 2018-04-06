package com.scaleunlimited.cascading.scheme.core;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public abstract class SolrWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolrWriter.class);

    // TODO KKr - make this configurable.
    private static final int MAX_DOCS_PER_ADD = 500;

    private KeepAliveHook _keepAlive;

    private Fields _sinkFields;
    private int _maxSegments;
    
    private transient CoreContainer _coreContainer;
    private transient EmbeddedSolrServer _solrServer;
    private transient BinaryUpdateRequest _updateRequest;

    public SolrWriter(KeepAliveHook keepAlive, Fields sinkFields, String dataDir, File solrConfDir, int maxSegments) throws IOException {
        _keepAlive = keepAlive;
        _sinkFields = sinkFields;
        _maxSegments = maxSegments;
        
        _updateRequest = new BinaryUpdateRequest();
        // Set up overwrite=false. See https://issues.apache.org/jira/browse/SOLR-653
        // for details why we have to do it this way.
        _updateRequest.setParam(UpdateParams.OVERWRITE, Boolean.toString(false));

        // Fire up an embedded Solr server
        try {
            File solrHome = SolrSchemeUtil.makeTempSolrHome(solrConfDir, new File(dataDir));
            _coreContainer = new CoreContainer(solrHome.getAbsolutePath());
            _coreContainer.load();
            _solrServer = new EmbeddedSolrServer(_coreContainer, SolrSchemeUtil.CORE_DIR_NAME);
        } catch (Exception e) {
            if (_coreContainer != null) {
                _coreContainer.shutdown();
            }
            
            throw new IOException(e);
        }
    }
    
    public void add(Tuple value) throws IOException {
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
            } else if (fieldValue instanceof byte[]) {
                safeAdd(doc, name, fieldValue);
            } else if (fieldValue instanceof BytesWritable) {
                BytesWritable bw = (BytesWritable)fieldValue;
                byte[] binaryData = bw.getBytes();
                
                // See if the array we get back is longer than the actual data we've got.
                if (binaryData.length != bw.getLength()) {
                    byte[] truncatedData = new byte[bw.getLength()];
                    System.arraycopy(binaryData, 0, truncatedData, 0, bw.getLength());
                    safeAdd(doc, name, truncatedData);
                } else {
                    safeAdd(doc, name, binaryData);
                }
            } else {
                safeAdd(doc, name, fieldValue.toString());
            }
        }

        _updateRequest.add(doc);
        flushInputDocuments(false);
    }
    
    private void safeAdd(SolrInputDocument doc, String fieldName, Object value) {
        if (value != null) {
            doc.addField(fieldName, value);
        }
    }
    
    private void flushInputDocuments(boolean force) throws IOException {
        if ((force && (_updateRequest.getDocListSize() > 0)) || (_updateRequest.getDocListSize() >= MAX_DOCS_PER_ADD)) {
            
            // TODO do we need to do this?
            Thread reporterThread = startProgressThread();

            try {
                _updateRequest.process(_solrServer);
                
                if (force) {
                    _solrServer.commit(SolrSchemeUtil.CORE_DIR_NAME, true, true);
                    _solrServer.optimize(SolrSchemeUtil.CORE_DIR_NAME, true, true, _maxSegments);
                }
            } catch (SolrServerException e) {
                throw new IOException(e);
            } finally {
                _updateRequest.clear();
                reporterThread.interrupt();
            }
        }
    
    }

    public void cleanup() throws IOException {
        flushInputDocuments(true);
        _coreContainer.shutdown();
        _solrServer = null;
    }
    
    /**
     * Fire off a thread that repeatedly calls Cascading to tell it we're making progress.
     * @return
     */
    private Thread startProgressThread() {
        Thread result = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    _keepAlive.keepAlive();
                    
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
