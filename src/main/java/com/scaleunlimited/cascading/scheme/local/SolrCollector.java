package com.scaleunlimited.cascading.scheme.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;

import com.scaleunlimited.cascading.scheme.core.SolrSchemeUtil;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class SolrCollector {

    // TODO KKr - make this configurable.
    private static final int MAX_DOCS_PER_ADD = 500;

    private FlowProcess<Properties> _flowProcess;
    private Fields _sinkFields;
    private int _maxSegments;
    private String _dataDirPropertyName;
    
    private transient CoreContainer _coreContainer;
    private transient SolrServer _solrServer;
    private transient List<SolrInputDocument> _inputDocs;

    public SolrCollector(FlowProcess<Properties> flowProcess, Fields sinkFields, File solrCoreDir, int maxSegments, String dataDirPropertyName, String dataDir) throws IOException {
        _flowProcess = flowProcess;
        _sinkFields = sinkFields;
        _maxSegments = maxSegments;
        _dataDirPropertyName = dataDirPropertyName;
        
        _inputDocs = new ArrayList<SolrInputDocument>(MAX_DOCS_PER_ADD);

        // Fire up an embedded Solr server
        try {
            System.setProperty("solr.solr.home", SolrSchemeUtil.makeTempSolrHome(solrCoreDir).getAbsolutePath());
            System.setProperty(_dataDirPropertyName, dataDir);
            CoreContainer.Initializer initializer = new CoreContainer.Initializer();
            _coreContainer = initializer.initialize();
            _solrServer = new EmbeddedSolrServer(_coreContainer, solrCoreDir.getName());
        } catch (Exception e) {
            if (_coreContainer != null) {
                _coreContainer.shutdown();
            }
            
            throw new IOException(e);
        }
    }
    
    public void collect(Tuple value) throws IOException {
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

        _inputDocs.add(doc);
        flushInputDocuments(false);
    }
    
    private void safeAdd(SolrInputDocument doc, String fieldName, String value) {
        if ((value != null) && (value.length() > 0)) {
            doc.addField(fieldName, value);
        }
    }
    
    private void flushInputDocuments(boolean force) throws IOException {
        if ((force && (_inputDocs.size() > 0)) || (_inputDocs.size() >= MAX_DOCS_PER_ADD)) {
            
            // TODO do we need to do this?
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
                    _flowProcess.keepAlive();
                    
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
