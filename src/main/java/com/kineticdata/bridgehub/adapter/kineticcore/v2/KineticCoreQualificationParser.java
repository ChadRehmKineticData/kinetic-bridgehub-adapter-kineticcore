package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.QualificationParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KineticCoreQualificationParser extends QualificationParser {
     /** Defines the logger */
    protected static final org.slf4j.Logger logger 
        = LoggerFactory.getLogger(KineticCoreAdapter.class);
    
    protected List<NameValuePair> parseQuery (String queryString)
        throws BridgeError {
        
        // Split the query from the rest of the string
        String[] parts = queryString.split("[?]",2);
        
        List<NameValuePair> queryList = new ArrayList<>();

        if (parts.length > 1) {
            // Split into individual queries by splitting on the & between each 
            // distinct query
            String[] queries = parts[1].split("&(?=[^&]*?=)");
            for (String query : queries) {
                // Split the query on the = to determine the field/value key-pair. 
                // Anything before the first = is considered to be the field and 
                // anything after (including more = signs if there are any) is 
                // considered to be part of the value
                String[] str_array = query.split("=",2);
                String field = str_array[0].trim();
                String value = str_array[1].trim();

                queryList.add( new BasicNameValuePair(field, value.trim()));
            }
        }
        
        return queryList;
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