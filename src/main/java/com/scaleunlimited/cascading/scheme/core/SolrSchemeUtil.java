package com.scaleunlimited.cascading.scheme.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
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

    public static File makeTempSolrHome(File solrCoreDir, String dataDirPropertyName, File dataDir) throws IOException {
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpSolrHome = new File(tmpFolder, UUID.randomUUID().toString());
        
        // Set up a temp location for Solr home, where we're write out a synthetic solr.xml
        // that references the core directory.
        String coreName = solrCoreDir.getName();
        File tmpSolrCoreDir = new File(tmpSolrHome, coreName);
        FileUtils.copyDirectory(solrCoreDir, tmpSolrCoreDir);
        
        String solrXmlContent = String.format("<solr><cores><core name=\"%s\" instanceDir=\"%s\"></core></cores></solr>",
                                              coreName, tmpSolrCoreDir.getAbsolutePath());
        File solrXmlFile = new File(tmpSolrHome, "solr.xml");
        FileUtils.write(solrXmlFile, solrXmlContent);

        // Write out data dir location property, so we don't use a system property
        File confDir = new File(tmpSolrCoreDir, "conf");
        confDir.mkdirs();
        File confPropertiesFile = new File(confDir, "solrcore.properties");
        
        Properties p = new Properties();
        p.setProperty(dataDirPropertyName, dataDir.getAbsolutePath());
        FileOutputStream fos = new FileOutputStream(confPropertiesFile);
        p.store(fos, null);
        fos.close();

        return tmpSolrHome;
    }
    
    public static CoreContainer makeCoreContainer(File solrCoreDir, String dataDirPropertyName, File dataDir) throws IOException {
        File solrHomeDir = SolrSchemeUtil.makeTempSolrHome(solrCoreDir, dataDirPropertyName, dataDir);
        File configFile = new File(solrHomeDir, "solr.xml");
        
        System.setProperty("enable.special-handlers", "false"); // All we need is the update request handler
        System.setProperty("enable.cache-warming", "false"); // We certainly don't need to warm the cache
    
        // Don't use MMapDirectory, as that makes Hadoop 2/YARN think that we're using huge amounts of
        // permanent memory (the memory-mapped index files look like perm-mem that's "owned" by us).
        System.setProperty("solr.directoryFactory", "solr.SimpleFSDirectoryFactory");
        
        return new CoreContainer(solrHomeDir.getAbsolutePath(), configFile);
    }
    
    public static void validate(File solrCoreDir, String dataDirPropertyName, Fields schemeFields) throws IOException {
        
        // Verify solrHomeDir exists
        if (!solrCoreDir.exists() || !solrCoreDir.isDirectory()) {
            throw new TapException("Solr core directory doesn't exist: " + solrCoreDir);
        }
        
        // Set up a temp location for data, so when we instantiate the coreContainer,
        // we don't pollute the solr home with a /data sub-dir.
        String tmpFolder = System.getProperty("java.io.tmpdir");
        File tmpDataDir = new File(tmpFolder, "cascading.solr-" + UUID.randomUUID());
        tmpDataDir.mkdir();

        CoreContainer coreContainer = null;
        
        try {
            coreContainer = makeCoreContainer(solrCoreDir, dataDirPropertyName, tmpDataDir);
            Collection<SolrCore> cores = coreContainer.getCores();
            SolrCore core = null;
            
            if (cores.size() == 0) {
                throw new TapException("No Solr cores are available");
            } else if (cores.size() > 1) {
                throw new TapException("Only one Solr core is supported");
            } else {
                core = cores.iterator().next();
            }

            IndexSchema schema = core.getSchema();
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
