package com.scaleunlimited.cascading.scheme.core;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import cascading.tap.TapException;
import cascading.tuple.Fields;

public class SolrSchemeUtil {

    public static final String DEFAULT_DATA_DIR_PROPERTY_NAME = "solr.data.dir";

    public static File makeTempSolrHome(File solrCoreDir) throws IOException {
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpSolrHome = new File(tmpFolder, UUID.randomUUID().toString());

        // Set up a temp location for Solr home, where we're write out a synthetic solr.xml
        URL solrXmlContent = SolrSchemeUtil.class.getResource("default-solr.xml");
        File solrXmlFile = new File(tmpSolrHome, "solr.xml");
        FileUtils.copyURLToFile(solrXmlContent, solrXmlFile);
        FileUtils.copyDirectory(solrCoreDir, new File(tmpSolrHome, solrCoreDir.getName()));

        return tmpSolrHome;
    }
    
    public static void validate(File solrCoreDir, String dataDirPropertyName, Fields schemeFields) throws IOException {
        
        // Verify solrHomeDir exists
        if (!solrCoreDir.exists() || !solrCoreDir.isDirectory()) {
            throw new TapException("Solr core directory doesn't exist: " + solrCoreDir);
        }
        
        File tmpSolrHome = makeTempSolrHome(solrCoreDir);

        // Set up a temp location for data, so when we instantiate the coreContainer,
        // we don't pollute the solr home with a /data sub-dir.
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpDataDir = new File(tmpFolder, UUID.randomUUID().toString());
        tmpDataDir.mkdir();
        
        System.setProperty(dataDirPropertyName, tmpDataDir.getAbsolutePath());
        System.setProperty("enable.special-handlers", "false"); // All we need is the update request handler
        System.setProperty("enable.cache-warming", "false"); // We certainly don't need to warm the cache
        
        CoreContainer coreContainer = new CoreContainer(tmpSolrHome.getAbsolutePath());
        
        try {
            coreContainer.load();
            Collection<SolrCore> cores = coreContainer.getCores();
            SolrCore core = null;
            
            if (cores.size() == 0) {
                throw new TapException("No Solr cores are available");
            } else if (cores.size() > 1) {
                throw new TapException("Only one Solr core is supported");
            } else {
                core = cores.iterator().next();
            }

            IndexSchema schema = core.getLatestSchema();
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
}
