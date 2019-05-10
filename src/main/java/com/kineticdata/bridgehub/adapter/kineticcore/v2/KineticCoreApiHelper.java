package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import static com.kineticdata.bridgehub.adapter.kineticcore.v2.KineticCoreAdapter.logger;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
    private static final Pattern NESTED_PATTERN = Pattern.compile("(.*?)\\[(.*?)\\]");

    public KineticCoreApiHelper (String username, String password,
        String spaceUrl) {
        
        this.username = username;
        this.password = password;
        this.spaceUrl = spaceUrl;
    }
    
    public Count count(BridgeRequest request, 
        KineticCoreAdapter.Mapping mapping) throws BridgeError {
        
        String responce = executeRequest(request, mapping.getImplicitIncludes());
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray pluralResult = (JSONArray)json.get(mapping.getPlural());
        
        String nextPageToken = String.valueOf(json.getOrDefault("nextPageToken",
            null));
        
        Map<String,String> metadata = new LinkedHashMap<String,String>();

        metadata.put("pageToken",nextPageToken);

        // Create count object
        return new Count(pluralResult.size(), metadata);
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
                throw new BridgeError("Retrieve may only return one " 
                    + request.getStructure() + ". Please check query");
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
        
        List<Record> records = (pluralResult == null)
            ? Collections.emptyList()
            : createRecords(request.getFields(), pluralResult);

        String nextPageToken = String.valueOf(json.getOrDefault("nextPageToken",
            null));
        
        Map<String,String> metadata = new LinkedHashMap<String,String>();

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

    private Record createRecord(List<String> fields, JSONObject item) {
        Map<String,Object> record = new LinkedHashMap<String,Object>();
        
        fields.forEach(field -> {
            Matcher matcher = NESTED_PATTERN.matcher(field);
            
            if (matcher.matches()) {
                String collectionProperty = matcher.group(1);
                String collectionKey = matcher.group(2);

                Object collection = item.get(collectionProperty); // "attributes"
                String value;
                if (collection instanceof JSONArray) {
                    value = extract((JSONArray)collection, collectionKey);
                } else if (collection instanceof JSONObject) {
                    value = extract((JSONObject)collection, collectionKey);
                } else {
                    throw new RuntimeException("Unexpected nested property type for \""+field+"\".");
                }
                record.put(field, value);
            } else {
                record.put(field, extract(item, field));
            }
        });
        
        return new Record(record);
    }

    protected List<Record> createRecords(List<String> fields, JSONArray array) 
        throws BridgeError {
      // For each of the API result item
       return (List<Record>) array.stream()
        .map(item -> createRecord(fields, (JSONObject) item))
        .collect(Collectors.toList());
    }


    private String extract(JSONArray object, String key) {
        Object matchingItem = object.stream()
            .filter(jsonObject -> jsonObject instanceof JSONObject)
            .filter(jsonObject -> ((JSONObject)jsonObject).containsKey("name") 
                && ((JSONObject)jsonObject).containsKey("values")
            )
            .filter(jsonObject -> 
                key.equals(((JSONObject)jsonObject).get("name"))
            )
            .findFirst()
            .orElse(null);
        return extract((JSONObject)matchingItem, "values");
    }

    private String extract(JSONObject object, String field) {
        Object value = (object == null) ? null : object.get(field);

        String result;
        if (value == null) {
            result = null;
        } else if (value instanceof JSONObject) {
            result = ((JSONObject)value).toJSONString();
        } else if (value instanceof JSONArray) {
            result = ((JSONArray)value).toJSONString();
        } else {
            result = value.toString();
        }
        return result;
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
                    "Not Found: %s not found at %s.", request.getStructure(),
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
        if (!processedParameters.containsKey("limit")) {
            processedParameters.put("limit", new BasicNameValuePair("limit",
                "1000"));
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
