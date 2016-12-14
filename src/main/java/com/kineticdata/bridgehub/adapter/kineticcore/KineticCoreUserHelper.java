package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import static com.kineticdata.bridgehub.adapter.kineticcore.KineticCoreAdapter.logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
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
        this.attributePattern = Pattern.compile("(.*?)\\[(.*?)\\]");
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
        String url = String.format("%s/app/api/v1/users/%s?include=attributes,profileAttributes",this.spaceUrl,username);

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
        
        // Sort the records because they are always returned on one page
        if (request.getMetadata("order") == null) {
            // name,type,desc assumes name ASC,type ASC,desc ASC
            Map<String,String> defaultOrder = new LinkedHashMap<String,String>();
            for (String field : request.getFields()) {
                defaultOrder.put(field, "ASC");
            }
            records = sortRecords(defaultOrder, records);
        } else {
        // Creates a map out of order metadata
          Map<String,String> orderParse = BridgeUtils.parseOrder(request.getMetadata("order"));
          records = sortRecords(orderParse, records);
        }
        
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
    
    /**
       * Returns the string value of the object.
       * <p>
       * If the value is not a String, a JSON representation of the object will be returned.
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

    // A helper method used to call createRecordsFromUsers but with a 
    // single record instead of an array
    private Record createRecordFromUser(List<String> fields, JSONObject user) throws BridgeError {
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(user);
        return createRecordsFromUsers(fields,jsonArray).get(0);
    }

    // Made protected and final for the purposes of testing
    protected final List<Record> createRecordsFromUsers(List<String> fields, JSONArray users) throws BridgeError {
        // Go through the users in the JSONArray to create a list of records
        List<Record> records = new ArrayList<Record>();
        for (Object o : users) {
            JSONObject user = (JSONObject)o;
            Map<String,Object> record = new LinkedHashMap<String,Object>();
            for (String field : fields) {
                Matcher m = this.attributePattern.matcher(field);
                if (m.find()) {
                    record.put(field,toString(getAttributeValues(m.group(1),m.group(2),user)));
                } else {
                    record.put(field,toString(user.get(field)));
                }
            }
            records.add(new Record(record));
        }

        return records;
    }

    // Filter users was made protected for the purposes of testing
    private JSONArray searchUsers(BridgeRequest request) throws BridgeError {
        // Initializing the Http Objects
        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        
        // Based on the passed fields figure out if an ?include needs to be in the Url
        String includeParam = null;
        if (request.getFields() != null) {
            Boolean includeAttributes = false;
            Boolean includeProfileAttributes = false;
            for (String field : request.getFields()) {
                Matcher m = attributePattern.matcher(field);
                String attributeType = null;
                if (m.matches()) {
                    attributeType = m.group(1);
                }
                if (field.equals("attributes")) {
                    includeAttributes = true;
                }
                else if (field.equals("profileAttributes")) {
                    includeProfileAttributes = true;
                }
                else if (attributeType != null) {
                    if (attributeType.equals("attributes")) {
                        includeAttributes = true;
                    } else if (attributeType.equals("profileAttributes")) {
                        includeProfileAttributes = true;
                    }
                }
                if (includeAttributes && includeProfileAttributes) break;
            }
            if (includeAttributes) includeParam = "include=attributes";
            if (includeProfileAttributes) includeParam = includeParam == null ? "include=profileAttributes" : includeParam + ",profileAttributes";
            // If request.getFields() has a field in common with the detail fields list, include details
            if (!Collections.disjoint(DETAIL_FIELDS, request.getFields())) includeParam = includeParam == null ? "include=details" : includeParam + ",details";
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

    private Pattern getPatternFromValue(String value) {
        // Escape regex characters from value
        String[] parts = value.split("(?<!\\\\)%");
        for (int i = 0; i<parts.length; i++) {
            if (!parts[i].isEmpty()) parts[i] = Pattern.quote(parts[i].replaceAll("\\\\%","%"));
        }
        String regex = StringUtils.join(parts,".*?");
        if (!value.isEmpty() && value.substring(value.length() - 1).equals("%")) regex += ".*?";
        return Pattern.compile("^"+regex+"$",Pattern.CASE_INSENSITIVE);
    }
    
    private List getAttributeValues(String type, String name, JSONObject user) throws BridgeError {
        if (!user.containsKey(type)) throw new BridgeError(String.format("The field '%s' cannot be found on the User object",type));
        JSONArray attributes = (JSONArray)user.get(type);
        for (Object attribute : attributes) {
            HashMap attributeMap = (HashMap)attribute;
            if (((String)attributeMap.get("name")).equals(name)) {
                return (List)attributeMap.get("values");
            }
        }
        return new ArrayList(); // Return an empty list if no values were found
    }
    
    protected final JSONArray filterUsers(JSONArray users, String query) throws BridgeError {
        String[] queryParts = query.split("&");
        
        Map<String,Object[]> queryMatchers = new HashMap<String,Object[]>();
        for (String part : queryParts) {
            String[] split = part.split("=");
            String field = split[0].trim();
            String value = split.length > 1 ? split[1].trim() : "";
            
            Object[] matchers;
            if (value.equals("true") || value.equals("false")) {
                matchers = new Object[] { getPatternFromValue(value), Boolean.valueOf(value) };
            } else if (value.equals("null")) {
                matchers = new Object[] { null, getPatternFromValue(value) };
            } else if (value.isEmpty()) {
                matchers = new Object[] { "" };
            } else {
                matchers = new Object[] { getPatternFromValue(value) };
            }
            queryMatchers.put(field,matchers);
        }
        
        // Start with a full list of users and then delete from the list when they don't match
        // a qualification. Will be left with a list of values that match all qualifications.
        JSONArray matchedUsers = users;        
        for (Map.Entry<String,Object[]> entry : queryMatchers.entrySet()) {
            // If passed in field is an attribute, save its attributeName
            String attributeType = null;
            String attributeName = null;
            Matcher m = this.attributePattern.matcher(entry.getKey());
            if (m.find()) {
                attributeType = m.group(1);
                attributeName = m.group(2);
            }
            
            JSONArray matchedUsersEntry = new JSONArray();
            for (Object o : matchedUsers) {
                JSONObject user = (JSONObject)o;
                // Get the value for the field
                List fieldValues = attributeName != null ? getAttributeValues(attributeType,attributeName,user) : Arrays.asList(new Object[] { user.get(entry.getKey()) });
                
                // if field values is empty, check for an empty value
                if (fieldValues.isEmpty()) {
                    for (Object value : entry.getValue()) {
                        if (value.equals("")) matchedUsersEntry.add(o);
                    }
                } else {
                    for (Object fieldValue : fieldValues) {
                        for (Object value : entry.getValue()) {
                            if (fieldValue == value || // Objects equal
                               fieldValue != null && value != null && (
                                   value.getClass() == Pattern.class && ((Pattern)value).matcher(fieldValue.toString()).matches() || // fieldValue != null && Pattern matches
                                   value.equals(fieldValue) // fieldValue != null && values equal
                               )
                            ) { matchedUsersEntry.add(o); }
                        }
                    }
                }
            }
            matchedUsers = (JSONArray)matchedUsersEntry;
        }
        
        return matchedUsers;
    }
    
    protected List<Record> sortRecords(final Map<String,String> fieldParser, List<Record> records) throws BridgeError {
        Collections.sort(records, new Comparator<Record>() {
            @Override
            public int compare(Record r1, Record r2){
                CompareToBuilder comparator = new CompareToBuilder();

                for (Map.Entry<String,String> entry : fieldParser.entrySet()) {
                    String field = entry.getKey();
                    String order = entry.getValue();

                    Object o1 = r1.getValue(field);
                    Object o2 = r2.getValue(field);
                    // If the object is a type that cannot be sorted, continue to the next field
                    if (o1 instanceof List) { continue; }
                    if (o2 instanceof List) { continue; }
                    // If the object is a string, lowercase the string so that capitalization doesn't factor into the comparison
                    if (o1 != null && o1.getClass() == String.class) {o1 = o1.toString().toLowerCase();}
                    if (o2 != null && o2.getClass() == String.class) {o2 = o2.toString().toLowerCase();}

                    if (order.equals("DESC")) {
                        comparator.append(o2,o1);
                    } else {
                        comparator.append(o1,o2);
                    }
                }

                return comparator.toComparison();
            }
        });
        return records;
    }
}
