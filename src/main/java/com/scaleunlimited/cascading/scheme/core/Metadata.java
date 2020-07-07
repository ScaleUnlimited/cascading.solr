package com.scaleunlimited.cascading.scheme.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.codec.digest.DigestUtils;

public class Metadata {

    public static final String MD5_FILE_NAME = ".md5";

    public static File writeMetadata(File partDir) throws IOException {
        File md5File = new File(partDir, Metadata.MD5_FILE_NAME);
        OutputStream fos = new FileOutputStream(md5File);
        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
        try {
            File indexDir = new File(partDir, "index");
            File[] indexFiles = indexDir.listFiles();
            for (File indexFile : indexFiles) {
                InputStream is = new FileInputStream(indexFile);
                String md5 = null;
                try {
                    md5 = DigestUtils.md2Hex(is);
                } finally {
                    is.close();
                }
                osw.write(indexFile.getName() + "\t" + md5 + "\n");
            }
        } finally {
            osw.close();
        }
        return md5File;
    }
    
}
