package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KineticCoreAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/

    /** Defines the adapter display name. */
    public static final String NAME = "Kinetic Core v2 Bridge";

    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(KineticCoreAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(KineticCoreAdapter.class.getResourceAsStream("/"+KineticCoreAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+KineticCoreAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    /** Defines the collection of property names for the adapter. */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String SPACE_URL = "Kinetic Core Space Url";
    }
    private String username;
    private String password;
    private String spaceUrl;
    private KineticCoreApiHelper coreApiHelper;
    private KineticCoreQualificationParser parser;
    private static final Pattern NESTED_PATTERN = Pattern.compile("(.*?)\\[(.*?)\\]");

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.USERNAME).setIsRequired(true),
        new ConfigurableProperty(Properties.PASSWORD).setIsRequired(true).setIsSensitive(true),
        new ConfigurableProperty(Properties.SPACE_URL).setIsRequired(true)
    );


    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
       return VERSION;
    }

    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public void initialize() throws BridgeError {
        this.spaceUrl = properties.getValue(Properties.SPACE_URL);
        this.username = properties.getValue(Properties.USERNAME);
        this.password = properties.getValue(Properties.PASSWORD);
        this.coreApiHelper = new KineticCoreApiHelper(this.username, 
            this.password, this.spaceUrl);
        this.parser = new KineticCoreQualificationParser();

        // Testing the configuration values to make sure that they
        // correctly authenticate with Core
        testAuth();
    }
    
    public static class Mapping {
        private final String structure;
        private final String plural;
        private final String singular;
        private final Set<String> implicitIncludes;
        private List<String> paginationFields;
        
        public Mapping(String structure, String plural, String singular, 
            Collection<String> implicitIncludes) {
            
            this.structure = structure;
            this.plural = plural;
            this.singular = singular;
            this.implicitIncludes = new LinkedHashSet<>(implicitIncludes);
        }

        public Mapping(String structure, String plural, String singular, 
            Collection<String> implicitIncludes, 
            Collection<String> paginationFields) {
            
            this.structure = structure;
            this.plural = plural;
            this.singular = singular;
            this.implicitIncludes = new LinkedHashSet<>(implicitIncludes);
            this.paginationFields = new ArrayList<>(paginationFields);
        }
        
        public String getStructure() {
            return structure;
        }

        public String getPlural() {
            return plural;
        }

        public String getSingular() {
            return singular;
        }

        public Set<String> getImplicitIncludes() {
            return implicitIncludes;
        }
        
        public List<String> getPaginationFields() {
            return paginationFields;
        }
        
        public void setPaginationFields (List<String> paginationFields) {
            this.paginationFields = paginationFields;
        }
        
    }
    
    public static Map<String,Mapping> MAPPINGS 
        = new LinkedHashMap<String,Mapping>() {{
        
        put("Submissions", new Mapping("Submissions", "submissions", "submission",
            Arrays.asList("values","details"),
            Arrays.asList("closedAt","createdAt","submittedAt","updatedAt")));
        put("Forms", new Mapping("Forms", "forms", "form", 
            Arrays.asList("details", "attributes"),
            Arrays.asList("category", "createdAt", "name", "slug", "updatedAt",
                "status", "type")));
        put("Users", new Mapping("Users", "users", "user",
            Arrays.asList("attributes", "profileAttributes"),
            Arrays.asList("createdAt","displayName","email","updatedAt","username")));
        put("Teams", new Mapping("Teams", "teams", "team", 
            Arrays.asList("attributes","memberships","details"),
            Arrays.asList("created", "localName", "name", "updatedAt")));
        put("Kapps", new Mapping("Kapps", "kapps", "kapp", 
            Arrays.asList("details", "attributes"),
            Arrays.asList("createdAt", "name", "slug", "updateAt")));
        put("Datastore Forms", new Mapping("Datastore Forms", "forms", "form", 
            Arrays.asList("values","details"),
            Arrays.asList("createdAt", "name", "slug", "updatedAt", "status")));
        put("Datastore Submissions", new Mapping("Datastore Submissions", 
            "submissions", "submission", Arrays.asList("details", "attributes")));
    }};

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));
        
        Mapping mapping = getMapping(request.getStructure());
        
        Map<String, String> parameters = parser.getParameters(request.getQuery());
        
        if (!parameters.containsKey("limit")) {
            parameters.put("limit", "1000");
        }
        
        Map<String, NameValuePair> parameterMap = buildNameValuePairMap(parameters);
        
        String responce = coreApiHelper.executeRequest(request, 
            getUrl(request, parameterMap), parser);
        
        Map<String,String> metadata = new LinkedHashMap<String, String>();
        int count = 0;
        
        try {
            JSONObject json = (JSONObject)JSONValue.parse(responce);
            JSONArray pluralResult = new JSONArray();
            
            pluralResult = (JSONArray)json.get(mapping.getPlural());
            // Check if forms or form property was returned.
            if (pluralResult != null) {
                count = pluralResult.size();
            } else if ((JSONObject)json.get(mapping.getSingular()) != null) {
                count = 1;
            } else {
                count = 0;
            }
            
            String nextPageToken = String.valueOf(json.getOrDefault("nextPageToken",
                null));

            metadata.put("pageToken",nextPageToken);
        } catch (Exception e) {
            throw new BridgeError("There was an error Parsing the responce");
        }
        
        return new Count(count, metadata);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));

        Mapping mapping = getMapping(request.getStructure());

        Map<String, String> parameters = parser.getParameters(request.getQuery());

        parameters = addImplicitIncludes(parameters, mapping.getImplicitIncludes());
        
        Map<String, NameValuePair> parameterMap = buildNameValuePairMap(parameters);
        
        String responce = coreApiHelper.executeRequest(request, 
            getUrl(request, parameterMap), parser);
        
        JSONObject singleResult = new JSONObject();
        
        try {    
            JSONObject json = (JSONObject)JSONValue.parse(responce);
            JSONArray pluralResult = (JSONArray)json.get(mapping.getPlural());
            
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
        } catch (Exception e) {
            throw new BridgeError("There was an error Parsing the responce");
        }
        
        return createRecord(request.getFields(), singleResult);
    }

    /* 
     * The order of operation for sorting result:
     *  if No order metadata return results natural sort order.
     *  else
     *      Check if pagination is supported by the server.
     *      if Supported return results sorted from server.
     *          else
     *          if nextpagetoken == null
     *              Sort in the bridge
     *          else
     *              Set warning on metadata that results are not sorted.
     *              Return results in natural sort order. 
     */
    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));
        
        Mapping mapping = getMapping(request.getStructure());
        
        Map<String, String> parameters = parser.getParameters(request.getQuery());
        
        LinkedHashMap<String,String> sortOrderItems = null; 
        
        boolean paginationSupported = false;
        if (request.getMetadata("order") != null) {
            
            sortOrderItems = getSortOrderItems(
                BridgeUtils.parseOrder(request.getMetadata("order")));
            
            if (request.getStructure() == "Datastore Submissions") { 

                mapping.setPaginationFields(
                    Arrays.asList(parameters.get("index").split("\\s*,\\s*"))
                );
                paginationSupported = paginationSupported(mapping, sortOrderItems);  

            } else if (request.getStructure() == "Submissions" 
                || request.getStructure() == "Users"
                || request.getStructure() == "Teams") {

                if (!(sortOrderItems.size() == 1)) {
                    paginationSupported = paginationSupported(mapping, 
                        request.getQuery(),
                        // Get the only item in the map.
                        sortOrderItems.entrySet().iterator().next().getKey());
                }

            } else {
                paginationSupported = paginationSupported(mapping, 
                    request.getQuery());
            }
        }
        
        String limit = null;
        if (!paginationSupported || !parameters.containsKey("limit")) {    
            limit = parameters.get("limit");
            parameters.put("limit", "1000");
        }
        
        parameters = addImplicitIncludes(parameters, mapping.getImplicitIncludes());
        
        Map<String, NameValuePair> parameterMap = buildNameValuePairMap(parameters);
        
        String responce = coreApiHelper.executeRequest(request, 
            getUrl(request, parameterMap), parser);

        List<Record> records = new ArrayList<Record>();
        Map<String, String> metadata = request.getMetadata() != null ?
                request.getMetadata() : new HashMap<>();
        
        try {
            JSONObject json = (JSONObject)JSONValue.parse(responce);
            JSONArray pluralResult = (JSONArray)json.get(mapping.getPlural());
       
            records = (pluralResult == null)
                ? Collections.emptyList()
                : createRecords(request.getFields(), pluralResult);

            String nextPageToken = String.valueOf(json.getOrDefault("nextPageToken",
                null));
            metadata.put("pageToken", nextPageToken);
        } catch (Exception e) {
            throw new BridgeError("There was an error Parsing the responce");
        }
        
        
        // If core side sorting isn't supported and order is required.
        if (!paginationSupported && request.getMetadata("order") != null) {
            // If all the records have been retrived sort bridge side.
            if ( metadata.get("pageToken") != null) {
                
                int index = 0;
                int offset = records.size();
                if (limit != null) {
                    
                    Integer currentPage = 0;
                    if( metadata.get("currentPage") != null) {
                        try {
                            currentPage =
                                Integer.parseInt(metadata.get("currentPage"));
                        } catch (NumberFormatException e) {
                            throw new BridgeError("currentPage metadata must be"
                                + "an Integer");
                        }
                    }
                    
                    metadata.put("page", limit);
                    // increment page count if this isn't the first request.
                    metadata.put("currentPage", currentPage > 0 ?
                        "1" : Integer.toString(currentPage + 1));
                    
                    try {
                        offset = Integer.parseInt(limit);
                    } catch  (NumberFormatException e) {
                        throw new BridgeError("limit metadata must be"
                            + "an Integer");
                    }
                    index = (currentPage * offset);
                    offset = index + offset;
                }
                
                KappSubmissionComparator comparator =
                    new KappSubmissionComparator(sortOrderItems);
                Collections.sort(records, comparator);
                records = records.subList(index, (offset - 1));

            } else {
                metadata.put("warning", "Results won't be ordered because there "
                    + "was more than one page of results returned.");                

                logger.debug("Warning: Results won't be ordered because there "
                    + "was more than one page of results returned.");
            }
        }
        
        return new RecordList(request.getFields(), records, metadata);
    }

    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/
    protected Map<String, NameValuePair> buildNameValuePairMap(Map<String, String> parameters) {
        Map<String, NameValuePair> parameterMap = new HashMap<>();

        parameters.forEach((key, value) -> {
            parameterMap.put(key, new BasicNameValuePair(key, value));
        });
        
        return parameterMap;
    }
         
    protected Map<String, String> addImplicitIncludes(Map<String, String> parameters,
        Set<String> implicitIncludes) {
        
        if (parameters.containsKey("include")) {
            Set<String> includeSet = new LinkedHashSet<>();
            includeSet.addAll(Arrays.asList(parameters.get("include")
                .split("\\s*,\\s*")));

            includeSet.addAll(implicitIncludes);
        } else {
            parameters.put("include", 
                implicitIncludes.stream().collect(Collectors.joining(",")));
        }
        
        return parameters;
    }
    
    private LinkedHashMap<String, String> 
        getSortOrderItems (Map<String, String> uncastSortOrderItems)
        throws IllegalArgumentException{
        
        /* results of parseOrder does not allow for a structure that 
         * guarantees order.  Casting is required to preserver order.
         */
        if (!(uncastSortOrderItems instanceof LinkedHashMap)) {
            throw new IllegalArgumentException("MESSAGE");
        }
        
        return (LinkedHashMap)uncastSortOrderItems;
    }
        
    protected Mapping getMapping (String structure) throws BridgeError{
        Mapping mapping = MAPPINGS.get(structure);
        if (mapping == null) {
            throw new BridgeError("Invalid Structure: '" 
                + structure + "' is not a valid structure");
        }
        return mapping;
    }
    
    protected List<Record> createRecords(List<String> fields, JSONArray array) 
        throws BridgeError {
        
        // For each of the API result item
        return (List<Record>) array.stream()
            .map(item -> createRecord(fields, (JSONObject) item))
            .collect(Collectors.toList());
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
    
    protected String getUrl (BridgeRequest request,
        Map<String, NameValuePair> parameters) {
        
        return String.format("%s/app/api/v1/%s?%s", spaceUrl, 
            parser.parsePath(request.getQuery()), 
            URLEncodedUtils.format(parameters.values(), Charset.forName("UTF-8")));
    }
    
    // TODO: confirm that direction in order metadata matches qualification mapping. 
    protected boolean paginationSupported(Mapping mapping, String queryString, 
        String paginationField) {
        boolean supported = false;
        
        for(String field: mapping.getPaginationFields()) {
            if (supported && queryString.contains(field)) {
                supported = false;
                break;
            } else if (queryString.contains(field)) {
                supported = true;
            }
        }

        return supported;
    }
    
    // TODO: confirm that direction in order metadata matches qualification mapping.
    protected boolean paginationSupported(Mapping mapping, String queryString) {
        boolean supported = false;

        for(String field: mapping.getPaginationFields()) {
            if (queryString.contains(field)) {
                supported = true;
            }
        }
        
        return supported;
    }
    
    // TODO: confirm that direction in order metadata matches qualification mapping.
    protected boolean paginationSupported(Mapping mapping, 
        LinkedHashMap<String, String> sortOrderItems) {
        boolean supported = false;
        
        // check that Ordering has consistant direction.
        supported = sortOrderItems.values().stream().map(String::toLowerCase)
            .collect(Collectors.toSet()).size() <= 1;
        
        // If all sort fields are either ascending or descending continue chacking
        // if pagination is supported with all index in same order and quanity.
        if (sortOrderItems.size() == mapping.paginationFields.size()
                && supported) {
            
            int idx = 0;
            for (String field: sortOrderItems.keySet()) {
                if (!mapping.paginationFields.get(idx).equalsIgnoreCase(field)) {
                    supported = false;
                    break;
                }
                idx++;
            }
        } else {
            supported = false;
        }
        
        return supported;
    }
    
    private String substituteQueryParameters(BridgeRequest request) throws BridgeError {
        // Parse the query and exchange out any parameters with their parameter 
        // values. ie. change the query username=<%=parameter["Username"]%> to
        // username=test.user where parameter["Username"]=test.user
        KineticCoreQualificationParser parser = new KineticCoreQualificationParser();
        return parser.parse(request.getQuery(),request.getParameters());
    }

    private void testAuth() throws BridgeError {
        logger.debug("Testing the authentication credentials");
        HttpGet get = new HttpGet(spaceUrl + "/app/api/v1/space");
        get = addAuthenticationHeader(get, this.username, this.password);

        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            if (response.getStatusLine().getStatusCode() == 401) {
                throw new BridgeError("Unauthorized: The inputted Username/Password combination is not valid.");
            } else if (response.getStatusLine().getStatusCode() != 200) {
                throw new BridgeError("Connecting to the Kinetic Core instance located at '"+this.spaceUrl+"' failed.");
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to properly to Kinetic Core.");
        }
    }

    private HttpGet addAuthenticationHeader(HttpGet get, String username, String password) {
        String creds = username + ":" + password;
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));

        return get;
    }
}
