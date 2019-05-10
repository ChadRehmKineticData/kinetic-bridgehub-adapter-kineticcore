package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.QualificationParser;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KineticCoreQualificationParser extends QualificationParser {
     /** Defines the logger */
    protected static final org.slf4j.Logger logger 
        = LoggerFactory.getLogger(KineticCoreAdapter.class);
    
    public List<NameValuePair> parseQuery (String queryString) {
        // Split the query from the rest of the string
        String[] parts = queryString.split("[?]",2);
        
        return parts.length > 1 ? URLEncodedUtils.parse(parts[1], 
            Charset.forName("UTF-8")) : new ArrayList<NameValuePair>();
    }
    
    public String parsePath (String queryString) {
        // Split the api path from the rest of the string
        String[] parts = queryString.split("[?]",2);
        
        return parts[0];
    }
    
    @Override
    public String encodeParameter(String name, String value) {
        String result = null;
        if (value != null) {
            result = value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
        return result;
    }
}