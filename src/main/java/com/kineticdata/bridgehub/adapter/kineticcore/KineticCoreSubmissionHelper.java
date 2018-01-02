package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import static com.kineticdata.bridgehub.adapter.kineticcore.KineticCoreAdapter.logger;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 */
public class KineticCoreSubmissionHelper {
    private final String username;
    private final String password;
    private final String spaceUrl;
    private final Pattern fieldPattern;
    private final Pattern attributePattern;

    public KineticCoreSubmissionHelper(String username, String password, String spaceUrl) {
        this.username = username;
        this.password = password;
        this.spaceUrl = spaceUrl;
        this.fieldPattern = Pattern.compile("(\\S+)\\[(.*?)\\]");
        this.attributePattern = Pattern.compile("(.*?)\\[(.*?)\\]");
    }

    public Count count(BridgeRequest request) throws BridgeError {
       Integer count = countSubmissions(request,null);

        return new Count(count);
    }

    public Record retrieve(BridgeRequest request) throws BridgeError {
        String submissionId = null;
        if (request.getQuery().matches("[Ii][Dd]=.*?(?:$|&)")) {
            Pattern p = Pattern.compile("[Ii][Dd]=(.*?)(?:$|&)");
            Matcher m = p.matcher(request.getQuery());

            if (m.find()) {
                submissionId = m.group(1);
            }
        }

        String url;
        JSONObject submission;
        if (submissionId == null) {
            JSONObject response = searchSubmissions(request);
            JSONArray submissions = (JSONArray)response.get("submissions");

            if (submissions.size() > 1) {
                throw new BridgeError("Multiple results matched an expected single match query");
            } else if (submissions.isEmpty()) {
                submission = null;
            } else {
                submission = (JSONObject)submissions.get(0);
            }
        } else {
            url = String.format("%s/app/api/v1/submissions/%s?include=values,details",this.spaceUrl,submissionId);

            HttpClient client = HttpClients.createDefault();
            HttpResponse response;
            HttpGet get = new HttpGet(url);
            get = addAuthenticationHeader(get, this.username, this.password);

            String output = "";
            try {
                response = client.execute(get);

                logger.trace("Request response code: " + response.getStatusLine().getStatusCode());
                HttpEntity entity = response.getEntity();
                if (response.getStatusLine().getStatusCode() == 404) {
                    throw new BridgeError(String.format("Not Found: The submission with the id '%s' cannot be found.",submissionId));
                }
                output = EntityUtils.toString(entity);
            }
            catch (IOException e) {
                logger.error(e.getMessage());
                throw new BridgeError("Unable to make a connection to the Kinetic Core server.");
            }

            JSONObject json = (JSONObject)JSONValue.parse(output);
            submission = (JSONObject)json.get("submission");
        }

        return createRecordFromSubmission(request.getFields(), submission);
    }

    public RecordList search(BridgeRequest request) throws BridgeError {
        // Initialize the metadata variable that will be returned
        Map<String,String> metadata = new LinkedHashMap<String,String>();
        JSONObject response = searchSubmissions(request);
        JSONArray submissions = (JSONArray)response.get("submissions");

        List<Record> records = createRecordsFromSubmissions(request.getFields(), submissions);

        // Sort the records if they are all returned in a second page
        if (request.getMetadata("order") == null && response.get("nextPageToken") == null) {
            // name,type,desc assumes name ASC,type ASC,desc ASC
            Map<String,String> defaultOrder = new LinkedHashMap<String,String>();
            for (String field : request.getFields()) {
                defaultOrder.put(field, "ASC");
            }
            records = sortRecords(defaultOrder, records);
        } else if (response.get("nextPageToken") == null) {
          // Creates a map out of order metadata
          Map<String,String> orderParse = BridgeUtils.parseOrder(request.getMetadata("order"));
          // Check for any fields in the order metadata that aren't included in the field list
          for (String field : orderParse.keySet()) {
              if (!request.getFields().contains(field)) {
                  // If any fields are hit that are in the sort metadata and not the field list,
                  // rebuild the record list while including the sort fields in the included fields
                  Set<String> allFields = new HashSet<String>(request.getFields());
                  allFields.addAll(orderParse.keySet());
                  records = createRecordsFromSubmissions(new ArrayList<String>(allFields),submissions);
                  break;
              }
          }
          records = sortRecords(orderParse, records);
        } else {
            metadata.put("warning","Records won't be ordered because there is more than one page of results returned.");
            logger.debug("Warning: Records won't be ordered because there is more than one page of results returned.");
        }

        metadata.put("size", String.valueOf(submissions.size()));
        metadata.put("nextPageToken",(String)response.get("nextPageToken"));

        // Return the response
        return new RecordList(request.getFields(), records, metadata);
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

    // A helper method used to call createRecordsFromSubmissions but with a
    // single record instead of an array
    private Record createRecordFromSubmission(List<String> fields, JSONObject submission) throws BridgeError {
        Record record;
        if (submission != null) {
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(submission);
            record = createRecordsFromSubmissions(fields,jsonArray).get(0);
        } else {
            record = new Record();
        }
        return record;
    }

    private List<Record> createRecordsFromSubmissions(List<String> fields, JSONArray submissions) throws BridgeError {
        // Create 'searchable' field list. If there needs to be a multi-level
        // search (aka, values[Group]) the field's type will be List<String>
        // instead of just String
        List searchableFields = new ArrayList();
        for (String field : fields) {
            Matcher matcher = fieldPattern.matcher(field);
            if (matcher.find()) {
                List<String> multiLevelField = new ArrayList<String>();
                multiLevelField.add(matcher.group(1));
                multiLevelField.add(matcher.group(2));
                searchableFields.add(multiLevelField);
            } else {
                searchableFields.add(field);
            }
        }

        // Go through the submissions in the JSONArray to create a list of records
        List<Record> records = new ArrayList<Record>();
        for (Object o : submissions) {
            records.add(new Record((Map)o));
        }

        // Get any field values from a JSON object if the field is in the form of field[jsonKey]
        records = BridgeUtils.getNestedFields(fields,records);

        return records;
    }

    private Integer countSubmissions(BridgeRequest request, String pageToken) throws BridgeError {
        Integer count = 0;
        String[] indvQueryParts = request.getQuery().split("&");

        // Retrieving the slugs for the kapp and form slug that were passed in the query
        String kappSlug = null;
        String formSlug = null;
        String parentId = null;
        List<String> queryPartsList = new ArrayList<String>();
        for (String indvQueryPart : indvQueryParts) {
            String[] str_array = indvQueryPart.split("=");
            String field = str_array[0].trim();
            String value = "";
            if (str_array.length > 1) value = str_array[1].trim();
            if (field.equals("formSlug")) { formSlug = value; }
            else if (field.equals("kappSlug")) { kappSlug = value; }
            else if (field.equals("parent")) { parentId = value; }
            else if (!field.equals("limit")) { // ignore the limit, because count always uses the default limit
                queryPartsList.add(URLEncoder.encode(field) + "=" + URLEncoder.encode(value));
            }
        }

        if (parentId == null) {
            if (kappSlug == null) {
                throw new BridgeError("Invalid Request: The bridge query needs to include a kappSlug.");
            }
            
            queryPartsList.add("limit=1000");
            String query = StringUtils.join(queryPartsList,"&");

            // Make sure that the pageToken isn't null for the first pass.
            String nextToken = pageToken != null ? pageToken : "";
            while (nextToken != null) {
                // the token query is used to reset the query each time so that multiple pageTokens
                // aren't added to the query after multiple passes
                String tokenQuery = query;
                // if nextToken is empty, don't add to query (only relevant on first pass)
                if (!nextToken.isEmpty()) {
                    tokenQuery = tokenQuery+"&pageToken="+nextToken;
                }
                JSONObject json = searchSubmissions(kappSlug, formSlug, tokenQuery);
                nextToken = (String)json.get("nextPageToken");
                JSONArray submissions = (JSONArray)json.get("submissions");
                count += submissions.size();
            }
        } else {
            JSONArray children = getChildren(parentId);
            children = filterSubmissions(children,URLDecoder.decode(StringUtils.join(queryPartsList,"&")));
            count = children.size();
        }

        return count;
    }

        /**
       * Returns the string value of the object.
       * <p>
       * If the value is not a String, a JSON representation of the object will be returned.
       *
       * @param value
       * @return
       */
    private static String toString(Object value) {
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

    private JSONObject searchSubmissions(BridgeRequest request) throws BridgeError {
        String[] indvQueryParts = request.getQuery().split("&(?=[^&]*?=)");

        // Retrieving the slugs for the kapp and form slug that were passed in the query
        String kappSlug = null;
        String formSlug = null;
        String parentId = null;
        String limit = null;
        List<String> queryPartsList = new ArrayList<String>();
        for (String indvQueryPart : indvQueryParts) {
            String[] str_array = indvQueryPart.split("=");
            String field = str_array[0].trim();
            String value = "";
            if (str_array.length > 1) value = StringUtils.join(Arrays.copyOfRange(str_array, 1, str_array.length),"=");
            if (field.equals("formSlug")) { formSlug = value; }
            else if (field.equals("kappSlug")) { kappSlug = value; }
            else if (field.equals("parent")) { parentId = value; }
            else if (field.equals("limit")) { limit = value; }
            else {
                queryPartsList.add(URLEncoder.encode(field) + "=" + URLEncoder.encode(value.trim()));
            }
        }
        
        if (parentId == null) {
            // Add the include statement to get extra values and details
            queryPartsList.add("include=values,details");

            // Add a limit to the query by either using the value that was passed, or defaulting limit=200
            String pageSize = request.getMetadata("pageSize");
            if (pageSize != null) {
                queryPartsList.add("limit="+pageSize);
            } else if (limit != null && !limit.isEmpty()) {
                queryPartsList.add("limit="+limit);
            } else {
                queryPartsList.add("limit=1000");
            }

            // If metadata[nextPageToken] is included in the request, add it to the query
            if (request.getMetadata("pageToken") != null) {
                queryPartsList.add("pageToken="+request.getMetadata("pageToken"));
            }

            // Join the query list into a query string
            String query = StringUtils.join(queryPartsList,"&");

            if (kappSlug == null) {
                throw new BridgeError("Invalid Request: The bridge query needs to include a kappSlug.");
            }

            return searchSubmissions(kappSlug, formSlug, query);
        } else {
            // Create a sample JSONObject submissions return object containing a submissions array (the
            // result of the getChildren call) and a nextPageToken (always set to null) so that the logic in the
            // search method determining if a submission set is a single page is still valid
            Map<String,Object> json = new HashMap<String,Object>();
            json.put("nextPageToken",null);
            json.put("submissions",filterSubmissions(getChildren(parentId),URLDecoder.decode(StringUtils.join(queryPartsList,"&"))));
            return new JSONObject(json);
        }
    }

    private JSONObject searchSubmissions(String kapp, String form, String query) throws BridgeError {
        // Initializing the Http Objects
        HttpClient client = HttpClients.createDefault();
        HttpResponse response;

        // Build the submissions api url. Url is different based on whether the form slug has been included.
        String url;
        if (form != null) {
            url = String.format("%s/app/api/v1/kapps/%s/forms/%s/submissions?%s",this.spaceUrl,kapp,form,query);
        } else {
            url = String.format("%s/app/api/v1/kapps/%s/submissions?%s",this.spaceUrl,kapp,query);
        }
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

        if (response.getStatusLine().getStatusCode() == 404) {
            throw new BridgeError("Invalid kappSlug or formSlug: " + json.get("error").toString());
        } else if (response.getStatusLine().getStatusCode() != 200) {
            String errorMessage = json.containsKey("error") ? json.get("error").toString() : json.toJSONString();
            throw new BridgeError("Bridge Error: " + errorMessage);
        }

        JSONArray messages = (JSONArray)json.get("messages");

        if (!messages.isEmpty()) {
            logger.trace("Message from the Submissions API for the follwing query: "+query+"\n"+StringUtils.join(messages,"; "));
        }

        return json;
    }
    
    private JSONArray getChildren(String parentId) throws BridgeError {
        // Initializing the Http Objects
        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        String url = String.format("%s/app/api/v1/submissions/%s?include=children.details,children.values,children.form",this.spaceUrl,parentId);
        
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
            throw new BridgeError("Unable to make a connection to the Kinetic Core server.",e);
        }

        logger.trace("Starting to parse the JSON Response");
        JSONObject json = (JSONObject)JSONValue.parse(output);

        if (response.getStatusLine().getStatusCode() == 404) {
            throw new BridgeError("Invalid parent id: " + json.get("error").toString());
        } else if (response.getStatusLine().getStatusCode() != 200) {
            String errorMessage = json.containsKey("error") ? json.get("error").toString() : json.toJSONString();
            throw new BridgeError("Bridge Error: " + errorMessage);
        }
        
        JSONObject submission = (JSONObject)json.get("submission");
        JSONArray children = (JSONArray)submission.get("children");
        return children;
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
                    if (o1 != null) { o1 = KineticCoreSubmissionHelper.toString(o1).toLowerCase(); }

                    Object o2 = r2.getValue(field);
                    if (o2 != null) { o2 = KineticCoreSubmissionHelper.toString(o2).toLowerCase(); }

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

    private Object getChildValues(String type, String name, JSONObject submission) throws BridgeError {
        if (!submission.containsKey(type)) throw new BridgeError(String.format("The field '%s' cannot be found on the Submission object",type));
        Object child = submission.get(type);
        if (child instanceof JSONArray) {
            JSONArray attributes = (JSONArray)child;
            for (Object attribute : attributes) {
                HashMap attributeMap = (HashMap)attribute;
                if (((String)attributeMap.get("name")).equals(name)) {
                    return (List)attributeMap.get("values");
                }
            }
            return new ArrayList(); // Return an empty array list if nothing else was returned
        } else if (child instanceof JSONObject) {
            JSONObject values = (JSONObject)child;
            return values.get(name);
        } else {
            return child;
        }
    }
    
    protected final JSONArray filterSubmissions(JSONArray submissions, String query) throws BridgeError {
        if (query.isEmpty()) return submissions;
        String[] queryParts = query.split("&");

        Map<String[],Object[]> queryMatchers = new HashMap<String[],Object[]>();
        // Variables used for OR query (pattern and fields)
        String pattern = null;
        String[] fields = null;
        // Iterate through the query parts and create all the possible matchers to check against
        // the user results
        for (String part : queryParts) {
            String[] split = part.split("=");
            String field = split[0].trim();
            String value = split.length > 1 ? split[1].trim() : "";

            Object[] matchers;
            if (field.equals("pattern")) {
                pattern = value;
            } else if (field.equals("fields")) {
                fields = value.split(",");
            } else {
                // If the field isn't 'pattern' or 'fields', add the field and appropriate values
                // to the query matcher
                if (value.equals("true") || value.equals("false")) {
                    matchers = new Object[] { getPatternFromValue(value), Boolean.valueOf(value) };
                } else if (value.equals("null")) {
                    matchers = new Object[] { null, getPatternFromValue(value) };
                } else if (value.isEmpty()) {
                    matchers = new Object[] { "" };
                } else {
                    matchers = new Object[] { getPatternFromValue(value) };
                }
                queryMatchers.put(new String[] { field }, matchers);
            }
        }

        // If both query and pattern are not equal to null, add the list of fields and the
        // pattern (compiled into a regex Pattern object) to the queryMatchers map
        if (pattern != null && fields != null) {
            queryMatchers.put(fields,new Object[] { Pattern.compile(".*"+Pattern.quote(pattern)+".*",Pattern.CASE_INSENSITIVE) });
        }
        // If both pattern & fields are not equals to null AND both pattern & fields are not
        // both null, that means that one is null and the other is not which is not an
        // allowed query.
        else if (pattern != null || fields != null) {
            throw new BridgeError("The 'pattern' and 'fields' parameter must be provided together.  When the 'pattern' parameter "+
                    "is provided the 'fields' parameter is required and when the 'fields' parameter is provided the 'pattern' parameter is required.");
        }

        // Start with a full list of submissions and then delete from the list when they don't match
        // a qualification. Will be left with a list of values that match all qualifications.
        JSONArray matchedSubmissions = submissions;
        for (Map.Entry<String[],Object[]> entry : queryMatchers.entrySet()) {
            JSONArray matchedSubmissionsEntry = new JSONArray();
            for (String field : entry.getKey()) {
                // If passed in field is an attribute, save its attributeName
                String attributeType = null;
                String attributeName = null;
                Matcher m = this.attributePattern.matcher(field);
                if (m.find()) {
                    attributeType = m.group(1);
                    attributeName = m.group(2);
                }

                for (Object o : matchedSubmissions) {
                    JSONObject submission = (JSONObject)o;
                    // Check if the object matches the field qualification if it hasn't already been
                    // successfully matched
                    if (!matchedSubmissionsEntry.contains(o)) {
                        // Get the value for the field
                        List fieldValues;
                        if (attributeName != null) {
                            Object childValues = getChildValues(attributeType,attributeName,submission);
                            if (childValues instanceof List) {
                                fieldValues = (List)childValues;
                            } else {
                                fieldValues = Arrays.asList(new Object[] { childValues });
                            }
                        } else {
                            fieldValues = Arrays.asList(new Object[] { submission.get(field) });
                        }

                        // if field values is empty, check for an empty value
                        if (fieldValues.isEmpty()) {
                            for (Object value : entry.getValue()) {
                                if (value.equals("")) matchedSubmissionsEntry.add(o);
                            }
                        } else {
                            for (Object fieldValue : fieldValues) {
                                for (Object value : entry.getValue()) {
                                    if (fieldValue == value || // Objects equal
                                       fieldValue != null && value != null && (
                                           value.getClass() == Pattern.class && ((Pattern)value).matcher(fieldValue.toString()).matches() || // fieldValue != null && Pattern matches
                                           value.equals(fieldValue) // fieldValue != null && values equal
                                       )
                                    ) {
                                        matchedSubmissionsEntry.add(o);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            matchedSubmissions = (JSONArray)matchedSubmissionsEntry;
        }

        return matchedSubmissions;
    }
}
