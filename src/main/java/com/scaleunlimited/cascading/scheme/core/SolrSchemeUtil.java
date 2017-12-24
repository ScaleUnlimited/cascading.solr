package com.scaleunlimited.cascading.scheme.core;

import java.io.File;
import java.io.IOException;
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
    public static final String CORE_DIR_NAME = "core";

    public static File makeTempSolrHome(File solrConfDir) throws IOException {
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpSolrHome = new File(tmpFolder, UUID.randomUUID().toString());
        
        // Set up a temp location for Solr home, where we're write out a synthetic solr.xml
        // that references the core directory.
        File solrXmlFile = new File(tmpSolrHome, "solr.xml");
        FileUtils.write(solrXmlFile, "<solr></solr>", "UTF-8");
        File coreDir = new File(tmpSolrHome, CORE_DIR_NAME);
        coreDir.mkdirs();
        File coreProps = new File(coreDir, "core.properties");
        coreProps.createNewFile();
            
        File destDir = new File(coreDir, "conf");
        FileUtils.copyDirectory(solrConfDir, destDir);

        return tmpSolrHome;
    }
    
    public static void validate(File solrConfDir, String dataDirPropertyName, Fields schemeFields) throws IOException {
        
        // Verify solrConfDir exists
        if (!solrConfDir.exists() || !solrConfDir.isDirectory()) {
            throw new TapException("Solr conf directory doesn't exist: " + solrConfDir);
        }
        
        // Create a temp solr home dir with a solr.xml and core.properties file to work off.
        File tmpSolrHome = makeTempSolrHome(solrConfDir);
        
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
