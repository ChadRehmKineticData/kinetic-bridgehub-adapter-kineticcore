package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 */
public class KineticCoreAdapterTest extends BridgeAdapterTestBase {
    
    static String userRecordsMockData = null;

    @Override
    public String getConfigFilePath() {
        return "src/test/resources/bridge-config.yml";
    }
    
    @Override
    public Class getAdapterClass() {
        return KineticCoreAdapter.class;
    }
    
    @Test
    public void test_invalidBridgeConfiguration() {
        BridgeError error = null;
        
        Map<String,String> invalidConfiguration = new LinkedHashMap<String,String>();
        invalidConfiguration.put("Username", "badUsername");
        invalidConfiguration.put("Password", "badPassword");
        invalidConfiguration.put("Kinetic Core Space Url","https://rcedev.kineticdata.com/kinetic/internal");
        
        BridgeAdapter adapter = new KineticCoreAdapter();
        adapter.setProperties(invalidConfiguration);
        try {
            adapter.initialize();
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNotNull(error);
    }
    
    @Test
    public void test_invalidField() {
        BridgeError error = null;
        
        List<String> invalidFields = new ArrayList<String>();
        UUID randomField = UUID.randomUUID();
        invalidFields.add(randomField.toString());
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure(getStructure());
        request.setFields(invalidFields);
        request.setQuery(getSingleValueQuery());
        
        Record record = null;
        try {
            record = getAdapter().retrieve(request);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        assertNull(record.getRecord().get(randomField.toString()));
    }
    
    @Test
    public void test_invalidQuery() {
        BridgeError error = null;
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure(getStructure());
        request.setFields(getFields());
        request.setQuery("");
        
        try {
            getAdapter().search(request);
        } catch (BridgeError e) { error = e; }
        
        assertNotNull(error);
    }
    
    @Test
    public void test_blankFields() {
        BridgeError error = null;
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure(getStructure());
        request.setFields(new ArrayList());
        request.setQuery(getSingleValueQuery());
        
        try {
            getAdapter().retrieve(request);
        } catch (BridgeError e) { error = e; }
        
        assertNotNull(error);
    }
    
    // Testing User Filtering. Currently testing the child methods to the search call that
    // have been marked 'protected final' to be able to use consistent return data.
    
    @Test
    public void test_users_queryAttributeField() {
        BridgeError error = null;
        
        // Create empty user helper (don't need username,password, or url because we are
        // just using mock data instead of making calls to kinetic core
        KineticCoreUserHelper helper = new KineticCoreUserHelper("","","");
        String query = "attributes[First Name]=Jeff";
        
        JSONArray matchedUsers = null;
        try {
            matchedUsers = helper.filterUsers(buildUserRecordArray(),query);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        
        // Assert that only 1 user matched the query
        assertEquals(1,matchedUsers.size());
        // Check to see if the username is jeff.johnson@gmail.com, the user who should 
        // have the First Name attribute of Jeff
        JSONObject object = (JSONObject)matchedUsers.get(0);
        assertEquals("jeff.johnson@gmail.com",object.get("username").toString());
    }
    
    @Test
    public void test_users_queryProfileAttributeField() {
        BridgeError error = null;
        
        // Create empty user helper (don't need username,password, or url because we are
        // just using mock data instead of making calls to kinetic core
        KineticCoreUserHelper helper = new KineticCoreUserHelper("","","");
        String query = "profileAttributes[Task Administrator]=false";
        
        JSONArray matchedUsers = null;
        try {
            matchedUsers = helper.filterUsers(buildUserRecordArray(),query);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        
        // Assert that only 1 user matched the query
        assertEquals(1,matchedUsers.size());
        // Check to see if the username is jeff.johnson@gmail.com, the user who should 
        // have the Task Administrator profile attribute of false
        JSONObject object = (JSONObject)matchedUsers.get(0);
        assertEquals("jeff.johnson@gmail.com",object.get("username").toString());
    }
    
    @Test
    public void test_users_queryStandardField() {
        BridgeError error = null;
        
        // Create empty user helper (don't need username,password, or url because we are
        // just using mock data instead of making calls to kinetic core
        KineticCoreUserHelper helper = new KineticCoreUserHelper("","","");
        String query = "username=test.user";
        
        JSONArray matchedUsers = null;
        try {
            matchedUsers = helper.filterUsers(buildUserRecordArray(),query);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        
        // Assert that only 1 user matched the query
        assertEquals(1,matchedUsers.size());
        // Check to see if the username is test.user
        JSONObject object = (JSONObject)matchedUsers.get(0);
        assertEquals("test.user",object.get("username").toString());
    }
    
    @Test
    public void test_users_queryLeadingWildcard() {
        BridgeError error = null;
        
        // Create empty user helper (don't need username,password, or url because we are
        // just using mock data instead of making calls to kinetic core
        KineticCoreUserHelper helper = new KineticCoreUserHelper("","","");
        String query = "username=%@kineticdata.com";
        
        JSONArray matchedUsers = null;
        try {
            matchedUsers = helper.filterUsers(buildUserRecordArray(),query);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        
        // Check to see if the usernames returned end in @kineticdata.com
        for (Object o : matchedUsers) {
            JSONObject json = (JSONObject)o;
            assertTrue(json.get("username").toString().matches(".*?@kineticdata.com"));
        }
    }
    
    @Test
    public void test_users_queryTrailingWildcard() {
        BridgeError error = null;
        
        // Create empty user helper (don't need username,password, or url because we are
        // just using mock data instead of making calls to kinetic core
        KineticCoreUserHelper helper = new KineticCoreUserHelper("","","");
        String query = "username=test%";
        
        JSONArray matchedUsers = null;
        try {
            matchedUsers = helper.filterUsers(buildUserRecordArray(),query);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        
        // Check to see if the usernames returned end in @kineticdata.com
        for (Object o : matchedUsers) {
            JSONObject json = (JSONObject)o;
            assertTrue(json.get("username").toString().matches("test.*?"));
        }
    }
    
    @Test
    public void test_users_queryLeadingAndTrailingWildcard() {
        BridgeError error = null;
        
        // Create empty user helper (don't need username,password, or url because we are
        // just using mock data instead of making calls to kinetic core
        KineticCoreUserHelper helper = new KineticCoreUserHelper("","","");
        String query = "username=%user@kineticdata%";
        
        JSONArray matchedUsers = null;
        try {
            matchedUsers = helper.filterUsers(buildUserRecordArray(),query);
        } catch (BridgeError e) { error = e; }
        
        assertNull(error);
        
        // Check to see if the usernames returned end in @kineticdata.com
        for (Object o : matchedUsers) {
            JSONObject json = (JSONObject)o;
            assertTrue(json.get("username").toString().matches(".*?user@kineticdata.*?"));
        }
    }
    
    @Test
    public void test_form_queryBySlug() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kappSlug=services&slug=bomboo-provisioning-process");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList recordList = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_retrieve_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms/cleaning");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?q=name=*%22c%22 AND status=%22Active%22");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
    
    @Test
    public void test_form_limit() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?limit=1");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
   
    @Test
    public void test_form_query() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?limit=100&q=name=*%22c%22 AND status=%22Active%22");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_form_query_include() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?include=fields");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_form_no_query() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_retrieve_datastore_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Datastore Forms");
        request.setQuery("kapps/services/forms/cleaning");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_datastore_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Datastore Forms");
        request.setQuery("kapps/services/forms?q=name=*%22c%22 AND status=%22Active%22");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_datastore_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Datastore Forms");
        request.setQuery("kapps/services/forms");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
    
    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/
    
    /**
      The JSON array will be built from a mock JSON string with the following 5 users
      
        username: test.user
        email: test.user@acme.com
        display name: Test user
        attributes: [First Name = Test, Last Name = User]
        profileAttributes: []
        space admin: false
        enabled: false
        
        username: don.demo@kineticdata.com
        email: don.demo@kineticdata.com
        display name: Don Demo
        attributes: [Group = Fulfillment::IT]
        profileAttributes: [Task Administrator = true]
        space admin: false
        enabled: true
        
        username: prod.user@kineticdata.com
        email: prod.user@kineticdata.com
        display name: Production User
        attributes: []
        profileAttributes: [Task Administrator = true]
        space admin: true
        enabled: true
        
        username: jeff.johnson@gmail.com
        email: jeff.johnson@gmail.com
        display name: Jeff Johnson
        attributes: [First Name = [Jeff, Jeffery], Last Name = Johnson]
        profileAttributes: [Task Administrator = false]
        space admin: true
        enabled: true
        
        username: dev.user@kineticdata.dev
        email: dev.user@kineticdata.dev
        display name: Test user
        attributes: []
        profileAttributes: []
        space admin: true
        enabled: true
    */    
    public JSONArray buildUserRecordArray() {
        if (userRecordsMockData == null) {
            userRecordsMockData = "{\"users\":[{\"attributes\":[{\"values\":[\"Test\"],\"name\":\"First Name\"},{\"values\":[\"User\"],\"name\":\"Last Name\"}],\"profileAttributes\":[],\"displayName\":\"Test User\",\"email\":\"test.user@acme.com\",\"enabled\":false,\"spaceAdmin\":false,\"username\":\"test.user\"},{\"attributes\":[{\"values\":[\"Fulfillment::IT\"],\"name\":\"Group\"}],\"profileAttributes\":[{\"name\":\"Task Administrator\",\"values\":[\"true\"]}],\"displayName\":\"Don Demo\",\"email\":\"don.demo@kineticdata.com\",\"enabled\":true,\"spaceAdmin\":false,\"username\":\"don.demo@kineticdata.com\"},{\"attributes\":[],\"profileAttributes\":[{\"name\":\"Task Administrator\",\"values\":[\"true\"]}],\"displayName\":\"Production User\",\"email\":\"prod.user@kineticdata.com\",\"enabled\":true,\"spaceAdmin\":true,\"username\":\"prod.user@kineticdata.com\"},{\"attributes\":[{\"values\":[\"Jeff\",\"Jeffery\"],\"name\":\"First Name\"},{\"values\":[\"Johnson\"],\"name\":\"Last Name\"}],\"profileAttributes\":[{\"name\":\"Task Administrator\",\"values\":[\"false\"]}],\"displayName\":\"Jeff Johnson\",\"email\":\"jeff.johnson@gmail.com\",\"enabled\":true,\"spaceAdmin\":true,\"username\":\"jeff.johnson@gmail.com\"},{\"attributes\":[],\"profileAttributes\":[],\"displayName\":\"Development User\",\"email\":\"dev.user@kineticdata.dev\",\"enabled\":true,\"spaceAdmin\":true,\"username\":\"dev.user@kineticdata.dev\"}]}";
        }
        JSONObject jsonObject = (JSONObject)JSONValue.parse(userRecordsMockData);
        return (JSONArray)jsonObject.get("users");
    }
}