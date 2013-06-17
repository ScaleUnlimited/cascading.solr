package com.scaleunlimited.cascading.scheme.core;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.ContentStream;

@SuppressWarnings("serial")
public class BinaryUpdateRequest extends UpdateRequest {
    
    private BinaryRequestWriter requestWriter = null;
    
    public BinaryUpdateRequest()
    {
      super();
      
      requestWriter = new BinaryRequestWriter();
    }

    public int getDocListSize() {
        if (getDocuments() == null) {
            return 0;
        } else {
            return getDocuments().size();
        }
    }
    
    //---------------------------------------------------------------------------
    //---------------------------------------------------------------------------
    
    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
        ArrayList<ContentStream> streams = new ArrayList<ContentStream>( 1 );
        streams.add(requestWriter.getContentStream(this));
        return streams;
    }

    public String getXML() throws IOException {
        throw new IllegalStateException("Can't get XML when using binary protocol");
    }
    
    /**
     * @since solr 1.4
     */
    public void writeXML( Writer writer ) throws IOException {
        throw new IllegalStateException("Can't write XML when using binary protocol");
    }



}
