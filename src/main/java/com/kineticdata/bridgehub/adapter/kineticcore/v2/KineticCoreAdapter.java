package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        private final String plural;
        private final String singular;
        private final Set<String> implicitIncludes;

        public Mapping(String plural, String singular, Collection<String> implicitIncludes) {
            this.plural = plural;
            this.singular = singular;
            this.implicitIncludes = new LinkedHashSet<>(implicitIncludes);
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
        
    }
    
    public static Map<String,Mapping> MAPPINGS 
        = new LinkedHashMap<String,Mapping>() {{
        
        put("Submissions", new Mapping("submissions", "submission",
            Arrays.asList("values","details")));
        put("Forms", new Mapping("forms", "form", 
            Arrays.asList("details", "attributes")));
        put("Users", new Mapping("users", "user",
            Arrays.asList("attributes", "profileAttributes")));
        put("Teams", new Mapping("teams", "team", 
            Arrays.asList("attributes","memberships","details")));
        put("Kapps", new Mapping("kapps", "kapp", 
            Arrays.asList("details", "attributes")));
        put("Datastore Forms", new Mapping("forms", "form", 
            Arrays.asList("values","details")));
        put("Datastore Submissions", new Mapping("submissions", "submission",
            Arrays.asList("details", "attributes")));
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

        return recordList;
    }

    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/

    private String substituteQueryParameters(BridgeRequest request) throws BridgeError {
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
