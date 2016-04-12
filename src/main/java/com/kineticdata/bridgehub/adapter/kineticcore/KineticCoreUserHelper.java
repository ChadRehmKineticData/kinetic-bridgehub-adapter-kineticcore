package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import static com.kineticdata.bridgehub.adapter.kineticcore.KineticCoreAdapter.logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 */
public class KineticCoreUserHelper {
    private final String username;
    private final String password;
    private final String spaceUrl;
    private final Pattern attributePattern;
    
    public KineticCoreUserHelper(String username, String password, String spaceUrl) {
        this.username = username;
        this.password = password;
        this.spaceUrl = spaceUrl;
        this.attributePattern = Pattern.compile("attributes\\[(.*?)\\]");
    }
    
    public static final List<String> DETAIL_FIELDS = Arrays.asList(new String[] {
        "createdAt","createdBy","updatedAt","updatedBy"
    });
    
    public Count count(BridgeRequest request) throws BridgeError {
        JSONArray users = searchUsers(request);
        
        // Create count object
        return new Count(users.size());
    }
    
    public Record retrieve(BridgeRequest request) throws BridgeError {
        String username = null;
        if (request.getQuery().matches("[Uu]sername=.*?(?:$|&)")) {
            Pattern p = Pattern.compile("[Uu]sername=(.*?)(?:$|&)");
            Matcher m = p.matcher(request.getQuery());
        
            if (m.find()) {
                username = m.group(1);
            }
        }
        
        if (username == null) {		
            throw new BridgeError(String.format("Invalid Query: Could not find username in the following query '%s'. Query must be in the form of username={username}",request.getQuery()));		
        }

        JSONObject user;
        String url = String.format("%s/app/api/v1/users/%s",this.spaceUrl,username);

        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        HttpGet get = new HttpGet(url);
        get = addAuthenticationHeader(get, this.username, this.password);

        String output = "";
        try {
            response = client.execute(get);

            logger.trace("Request response code: " + response.getStatusLine().getStatusCode());
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() == 404) {
                throw new BridgeError(String.format("Not Found: The user with the username '%s' cannot be found.",username));
            }
            output = EntityUtils.toString(entity);
        } 
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Kinetic Core server."); 
        }

        JSONObject json = (JSONObject)JSONValue.parse(output);
        user = (JSONObject)json.get("user");
        
        return createRecordFromUser(request.getFields(), user);
    }
    
    public RecordList search(BridgeRequest request) throws BridgeError {
        JSONArray users = searchUsers(request);
        
        List<Record> records = createRecordsFromUsers(request.getFields(), users);
        
        // Return the response
        return new RecordList(request.getFields(), records);
    }
    
    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/
    
    private HttpGet addAuthenticationHeader(HttpGet get, String username, String password) {
        String creds = username + ":" + password;
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));

        return get;
    }

    // A helper method used to call createRecordsFromUsers but with a 
    // single record instead of an array
    private Record createRecordFromUser(List<String> fields, JSONObject user) throws BridgeError {
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(user);
        return createRecordsFromUsers(fields,jsonArray).get(0);
    }

    private List<Record> createRecordsFromUsers(List<String> fields, JSONArray users) throws BridgeError {
        // Go through the users in the JSONArray to create a list of records
        List<Record> records = new ArrayList<>();
        for (Object o : users) {
            JSONObject user = (JSONObject)o;
            Map<String,Object> record = new LinkedHashMap<>();
            for (String field : fields) {
                record.put(field,user.get(field));
            }
            records.add(new Record(record));
        }

        return records;
    }

    private JSONArray searchUsers(BridgeRequest request) throws BridgeError {
        // Initializing the Http Objects
        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        
        // Based on the passed fields figure out if an ?include needs to be in the Url
        String includeParam = null;
        if (request.getFields().contains("attributes")) {
            includeParam = "include=attributes";
        }
        if (!Collections.disjoint(DETAIL_FIELDS, request.getFields())) {
            // If they have a field in common, include details
            includeParam = includeParam == null ? "include=details" : includeParam + ",details";
        }
        
        String url = this.spaceUrl + "/app/api/v1/users";
        if (includeParam != null) url += "?"+includeParam;
        HttpGet get = new HttpGet(url);
        get = addAuthenticationHeader(get, this.username, this.password);
        
        String output = "";
        try {
            response = client.execute(get);
            
            HttpEntity entity = response.getEntity();
            output = EntityUtils.toString(entity);
            logger.trace("Request response code: " + response.getStatusLine().getStatusCode());
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Kinetic Core server."); 
        }
        
        logger.trace("Starting to parse the JSON Response");
        JSONObject json = (JSONObject)JSONValue.parse(output);
        
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new BridgeError("Bridge Error: " + json.toJSONString());
        }

        JSONArray users = (JSONArray)json.get("users");
        
        String query = request.getQuery();
        if (!query.isEmpty()) {
            users = filterUsers(users, request.getQuery());
        }

        return users;
    }
    
    private JSONArray filterUsers(JSONArray users, String query) throws BridgeError{
        String[] indvQueryParts = query.split("&");

        // Retrieving the slugs for the kapp and form slug that were passed in the query
        Map<String,Object[]> queryValues = new HashMap<>();
        for (String indvQueryPart : indvQueryParts) {
            String[] str_array = indvQueryPart.split("=");
            String field = str_array[0].trim();
            String value = "";
            if (str_array.length > 1) value = str_array[1].trim();
            
            // Used to search more complex objects (dates, attributes, boolean, etc.)
            Object[] possibleEqualObjects;
            if (value.equals("true") || value.equals("false")) {
                // If it is a possible boolean object, include both the string and boolean object
                possibleEqualObjects = new Object[] { value, Boolean.valueOf(value) };
            } else if (value.equals("null")) {
                possibleEqualObjects = new Object[] { value, null };
            } else {
                possibleEqualObjects = new Object[] { value };
            }
            queryValues.put(field, possibleEqualObjects);
        }
        
        JSONArray matchedUsers = users;
        for (Map.Entry<String,Object[]> entry : queryValues.entrySet()) {
            matchedUsers = matchFieldValues(matchedUsers, entry.getKey(), entry.getValue());
        }
        
        return matchedUsers;
    }
    
    private JSONArray matchFieldValues(JSONArray users, String field, Object[] possibleValues) {
        JSONArray matchedUsers = new JSONArray();
        
        // If passed in field is an attribute, save its attributeName
        String attributeName = null;
        Matcher m = this.attributePattern.matcher(field);
        if (m.find()) attributeName = m.group(1);
        
        for (Object o : users) {
            JSONObject user = (JSONObject)o;
            // Get the value for the field
            List fieldValues = new ArrayList();
            if (attributeName != null) {
                JSONArray attributes = (JSONArray)user.get("attributes");
                for (Object attribute : attributes) {
                    HashMap attributeMap = (HashMap)attribute;
                    if (((String)attributeMap.get("name")).equals(attributeName)) {
                        fieldValues = (List)attributeMap.get("values");
                        break;
                    }
                }
            } else {
                fieldValues.add(user.get(field));
            }
            
            for (Object fieldValue : fieldValues) {
                for (Object value : possibleValues) {
                    if (fieldValue == value) {
                        matchedUsers.add(o);
                    } else if (fieldValue != null && fieldValue.equals(value)) {
                        matchedUsers.add(o);
                    }
                }
            }
        }
        return matchedUsers;
    }
}
