package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import static com.kineticdata.bridgehub.adapter.kineticcore.KineticCoreAdapter.logger;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class KineticCoreApiHelper {
    private final String username;
    private final String password;
    private final String spaceUrl;
    private final Pattern attributePattern;

    public KineticCoreApiHelper (String username, String password,
        String spaceUrl) {
        
        this.username = username;
        this.password = password;
        this.spaceUrl = spaceUrl;
        this.attributePattern = Pattern.compile("(.*?)\\[(.*?)\\]");
    }
    
    public Count count(BridgeRequest request, 
        KineticCoreAdapter.Mapping mapping) throws BridgeError {
        
        String responce = executeRequest(request, mapping.getImplicitIncludes());
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray pluralResult = (JSONArray)json.get(mapping.getPlural());

        // Create count object
        return new Count(pluralResult.size());
    }
    
    public Record retrieve(BridgeRequest request, 
        KineticCoreAdapter.Mapping mapping) throws BridgeError {
        
        String responce = executeRequest(request, mapping.getImplicitIncludes());
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray pluralResult = (JSONArray)json.get(mapping.getPlural());
        
        JSONObject singleResult;
        // Check if forms or form property was returned.
        if (pluralResult != null) {
            if (pluralResult.size() > 1) {
                throw new BridgeError("Retrieve may only return one Form. "
                    + "Please check query");
            } else {
                singleResult = (JSONObject)pluralResult.get(0);
            }
        } else {
            singleResult = (JSONObject)json.get(mapping.getSingular());
        }
        
        return createRecord(request.getFields(), singleResult);
    }
    
    public RecordList search(BridgeRequest request, 
        KineticCoreAdapter.Mapping mapping) throws BridgeError {
        
        String responce = executeRequest(request, mapping.getImplicitIncludes());
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray pluralResult = (JSONArray)json.get(mapping.getPlural());

        List<Record> records = createRecords(request.getFields(), pluralResult);

        // Add pagination to the returned record list
        int pageToken = request.getMetadata("pageToken") == null 
            || request.getMetadata("pageToken").isEmpty() ? 0 
            : Integer.parseInt(new String(Base64.decodeBase64(
            request.getMetadata("pageToken"))));

        int limit = request.getMetadata("limit") == null 
            || request.getMetadata("limit").isEmpty() ? records.size() - pageToken 
            : Integer.parseInt(request.getMetadata("limit"));

        String nextPageToken = null;
        if (pageToken + limit < records.size()) nextPageToken 
            = Base64.encodeBase64String(
            String.valueOf(pageToken + limit).getBytes());

        records = records.subList(pageToken, pageToken + limit > records.size() 
            ? records.size() : pageToken+limit);

        Map<String,String> metadata = new LinkedHashMap<String,String>();
        metadata.put("size",String.valueOf(limit));
        metadata.put("pageToken",nextPageToken);

        // Return the response
        return new RecordList(request.getFields(), records, metadata);
    }
    
    /*--------------------------------------------------------------------------
     * HELPER METHODS
     *------------------------------------------------------------------------*/
    
    /**
       * Returns the string value of the object.
       * <p>
       * If the value is not a String, a JSON representation of the object will 
       * be returned.
       *
       * @param value
       * @return
       */
    private String toString(Object value) {
        String result = null;
        if (value != null) {
            if (String.class.isInstance(value)) {
                result = (String)value;
            } else {
                result = JSONValue.toJSONString(value);
            }
        }
        return result;
     }

    // A helper method used to call createRecordsFromForms but with a
    // single record instead of an array
    private Record createRecord(List<String> fields, JSONObject form) 
        throws BridgeError {
        
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(form);
        return createRecords(fields,jsonArray).get(0);
    }

    // Made protected and final for the purposes of testing
    protected final List<Record> createRecords(List<String> fields, 
        JSONArray objects) throws BridgeError {
        
// Go through the users in the JSONArray to create a list of records
        List<Record> records = new ArrayList<Record>();
        for (Object o : objects) {
            JSONObject object = (JSONObject)o;
            Map<String,Object> record = new LinkedHashMap<String,Object>();
            for (String field : fields) {
                Matcher m = this.attributePattern.matcher(field);
                if (m.find()) {
                    record.put(field,toString(getAttributeValues(m.group(1),
                        m.group(2),object)));
                } else {
                    record.put(field,toString(object.get(field)));
                }
            }
            records.add(new Record(record));
        }

        return records;
    }

    private List getAttributeValues(String type, String name, JSONObject object)
        throws BridgeError {
        
        if (!object.containsKey(type)) throw new BridgeError(
            String.format("The field '%s' cannot be found on the Form object",
            type));
        
        JSONArray attributes = (JSONArray)object.get(type);
        for (Object attribute : attributes) {
            HashMap attributeMap = (HashMap)attribute;
            if (((String)attributeMap.get("name")).equals(name)) {
                return (List)attributeMap.get("values");
            }
        }
        return new ArrayList(); // Return an empty list if no values were found
    }
        
        
    public String executeRequest (BridgeRequest request, 
        Set<String> implicitIncludes) throws BridgeError{
        
        // Parse the query and exchange out any parameters with their parameter 
        // values. ie. change the query username=<%=parameter["Username"]%> to
        // username=test.user where parameter["Username"]=test.user
        KineticCoreQualificationParser parser 
            = new KineticCoreQualificationParser();
        String queryString = parser.parse(request.getQuery(), 
            request.getParameters());
        
        // Get a List of the parameters without the "path"
        List<NameValuePair> parameters = parser.parseQuery(queryString);
 
        
        String url = String.format("%s/app/api/v1/%s?%s", this.spaceUrl, 
            parser.parsePath(queryString), buildQuery(parameters, 
                implicitIncludes));
        
        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        HttpGet get = new HttpGet(url);
        get = addAuthenticationHeader(get, this.username, this.password);

        String output = "";
        try {
            response = client.execute(get);

            logger.trace("Request response code: " + response.getStatusLine()
                .getStatusCode());
            
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() == 404) {
                throw new BridgeError(String.format(
                    "Not Found: Form not found at %s.",
                    String.join(",", parser.parsePath(queryString))));
            }
            output = EntityUtils.toString(entity);
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError(
                "Unable to make a connection to the Kinetic Core server.");
        }
        
        return output;
    }
    
    protected String buildQuery (List<NameValuePair> parameters, 
        Set<String> implicitIncludes) {
        
        Map<String,NameValuePair> processedParameters = parameters.stream()
            .map(parameter -> {
                NameValuePair result;
                if ("include".equals(parameter.getName())) {
                    Set<String> includeSet = new LinkedHashSet<>();
                    includeSet.addAll(Arrays.asList(parameter.getValue()
                        .split("\\s*,\\s*")));
                    
                    includeSet.addAll(implicitIncludes);
                    result = new BasicNameValuePair("include", 
                        includeSet.stream().collect(Collectors.joining(",")));
                } else {
                    result = parameter;
                }
                return result;
            })
            .collect(Collectors.toMap(item -> item.getName(), item -> item));
        if (!processedParameters.containsKey("include")) {
            processedParameters.put("include", new BasicNameValuePair("include",
                implicitIncludes.stream().collect(Collectors.joining(","))));
        }
        
        return URLEncodedUtils.format(processedParameters.values(),
            Charset.forName("UTF-8"));
    } 
           
    private HttpGet addAuthenticationHeader(HttpGet get, String username,
        String password) {
        
        String creds = username + ":" + password;
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));

        return get;
    }
}
