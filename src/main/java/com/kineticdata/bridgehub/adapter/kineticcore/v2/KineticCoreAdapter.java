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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
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
        this.coreApiHelper = new KineticCoreApiHelper(this.username, this.password, this.spaceUrl);

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
        
        Mapping mapping = MAPPINGS.get(request.getStructure());
        if (mapping == null) {
            throw new BridgeError("Invalid Structure: '" 
                + request.getStructure() + "' is not a valid structure");
        }
        
        Count count = null;
        count = this.coreApiHelper.count(request, mapping);
        
        return count;
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));

        Mapping mapping = MAPPINGS.get(request.getStructure());
        if (mapping == null) {
            throw new BridgeError("Invalid Structure: '" 
                + request.getStructure() + "' is not a valid structure");
        }

        Record record;
        record = this.coreApiHelper.retrieve(request, mapping);

        return record;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));
        
        Mapping mapping = MAPPINGS.get(request.getStructure());
        if (mapping == null) {
            throw new BridgeError("Invalid Structure: '" 
                + request.getStructure() + "' is not a valid structure");
        }
        
        RecordList recordList;
        recordList = this.coreApiHelper.search(request, mapping);
        
        // Parse the query and exchange out any parameters with their parameter 
        // values. ie. change the query username=<%=parameter["Username"]%> to
        // username=test.user where parameter["Username"]=test.user
        KineticCoreQualificationParser parser 
            = new KineticCoreQualificationParser();
        String queryString = parser.parse(request.getQuery(), 
            request.getParameters());
        
        // get a List of the sort order items.
        Map<String,String> uncastSortOrderItems =
            BridgeUtils.parseOrder(request.getMetadata("order"));
                
        /* results of parseOrder does not allow for a structure that 
         * guarantees order.  Casting is required to preserver order.
         */
        if (!(uncastSortOrderItems instanceof LinkedHashMap)) {
            throw new IllegalArgumentException("MESSAGE");
        }
        LinkedHashMap<String,String> sortOrderItems =
            (LinkedHashMap)uncastSortOrderItems;

        boolean paginationSupported = false;
        if (request.getStructure() == "Datastore Submissions") {            
            mapping.setPaginationFields(getIndexs(queryString));
            paginationSupported = paginationSupported(mapping,
                request.getMetadata());
            
        } else if (request.getStructure() == "Submissions" 
            || request.getStructure() == "Users"
            || request.getStructure() == "Teams") {
            
            if (!(sortOrderItems.size() > 1)) {
                paginationSupported = paginationSupported(mapping, queryString, 
                    // Get the only item in the map.
                    sortOrderItems.entrySet().iterator().next().getKey());
            }
            
        } else {
             paginationSupported = paginationSupported(mapping, queryString);
        }

        if (!paginationSupported) {            
            // Sort the records if they are all returned.
            if (request.getMetadata("order") != null 
                && recordList.getMetadata().get("nextPageToken") == null) {

                KappSubmissionComparator comparator =
                    new KappSubmissionComparator(sortOrderItems);
                Collections.sort(recordList.getRecords(), comparator);
            } else {
                Map <String, String> metadata = new HashMap<String, String>();
                metadata.put("warning", "Results won't be ordered because there is "
                    + "more than one page of results returned.");                
                recordList.setMetadata (metadata);

                logger.debug("Warning: Results won't be ordered because there is "
                    + "more than one page of results returned.");
            }
        }
        
        return recordList;
    }

    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/
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
    
    protected boolean paginationSupported(Mapping mapping, String queryString) {
        boolean supported = false;

        for(String field: mapping.getPaginationFields()) {
            if (queryString.contains(field)) {
                supported = true;
            }
        }
        
        return supported;
    }
    
    protected boolean paginationSupported(Mapping mapping, 
        Map<String, String> metadata) {
        boolean supported = false;
        
        // check that Ordering has consistant direction.
        supported = metadata.values().stream().map(String::toLowerCase)
            .collect(Collectors.toSet()).size() <= 1;
        
        // If all sort fields are either ascending or descending continue chacking
        // if pagination is supported.
        if (supported) {
            int idx = 0;
            for (String field: metadata.keySet()) {
                if (mapping.paginationFields.get(idx).equalsIgnoreCase(field)) {
                    int x = 1;
                }
                idx++;
            }
        }
        
        return supported;
    }
    
    protected List<String> getIndexs(String queryString) {
        //Pattern to get all indexs in query.
        Pattern MY_PATTERN = Pattern.compile("(index=(.+)&)");
        Matcher m = MY_PATTERN.matcher(queryString);
        String indexs = null;
        while (m.find()) {
            indexs = m.group(2);
        }
        
        //return a List of indexs.
        return Arrays.asList(indexs.split("\\s*,\\s*"));
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
