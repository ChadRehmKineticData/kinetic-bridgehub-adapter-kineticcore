package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.kineticcore.v2.KineticCoreAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import org.junit.Assert;
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

    @Test
    public void test_retrieve_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms/cleaning");
        
        List<String> list = Arrays.asList("name", "slug", "attributes[Icon]");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?q=name=*\"c\" AND status=\"<%=parameter[\"Status\"]%>\"");
        
        Map parameters = new HashMap();
        parameters.put("Status", "Active");
        request.setParameters(parameters);
        
        List<String> list = Arrays.asList("name", "slug", "attributes[Icon]");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?q=name=*\"zzzz\"");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
    
    @Test
    public void test_form_limit() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?limit=100");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
   
    @Test
    public void test_form_query() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms?limit=100&q=name=*\"c\" AND status=\"Active\"");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_form_query_include() throws Exception {
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Forms");
        request.setQuery("kapps/services/forms/cleaning?include=fields");
        
        List<String> list = Arrays.asList("name", "slug", "fields");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
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
        request.setQuery("datastore/forms/alerts");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_datastore_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Datastore Forms");
        request.setQuery("datastore/forms?q=name=*\"a\" AND status=\"Active\"");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_datastore_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Datastore Forms");
        request.setQuery("datastore/forms");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
  
    @Test
    public void test_retrieve_users() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setQuery("users/chad.rehm%40kineticdata.com");
        
        List<String> list = Arrays.asList("displayName", "email", "profileAttributes[First Name]");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_users() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setQuery("users?limit=10&q=username=*\"c\" AND enabled=\"true\"");
        
        List<String> list = Arrays.asList("displayName", "email");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_temp_search_users() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        List<String> list = Arrays.asList("displayName", "email");
        request.setFields(list);
        
        request.setQuery("users?limit=10&q=username=*\"c\" AND enabled=\"true\"");
        RecordList records1 = getAdapter().search(request);
        
        request.setQuery("users?limit=10&q=username=*\"c\" AND enabled=\"true\"&pageToken=" + records1.getMetadata().get("pageToken"));
        RecordList records2 = getAdapter().search(request);
        
        request.setQuery("users?limit=20&q=username=*\"c\" AND enabled=\"true\"");
        RecordList records3 = getAdapter().search(request);
        
        String x = "1";
    }
    
    @Test
    public void test_count_users() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setQuery("users");
        
        List<String> list = Arrays.asList("displayName", "email");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
    
    @Test
    public void test_retrieve_teams() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Teams");
        request.setQuery("teams/c1ec1d62eafad32ca16fe4df49b9ca2f");
        
        List<String> list = Arrays.asList("name", "description");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_teams() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Teams");
        request.setQuery("teams?q=name=*\"a\"");
        
        List<String> list = Arrays.asList("name", "description", "memberships", "attributes[Icon]");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_teams() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Teams");
        request.setQuery("teams");
        
        List<String> list = Arrays.asList("name", "description");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
        
    @Test
    public void test_retrieve_kapps() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Kapps");
        request.setQuery("kapps/admin");
        
        List<String> list = Arrays.asList("name", "slug", "attributes[Bundle Package]");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_kapps() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Kapps");
        request.setQuery("kapps?q=name=*\"q\"");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_kapps() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Kapps");
        request.setQuery("kapps");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
    
        @Test
    public void test_retrieve_submissions() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Submissions");
        request.setQuery("submissions/5276a8fd-6509-11e9-b1c2-d5380975a038");
        
        List<String> list = Arrays.asList("createdBy", "label", "values[Checkbox Field]");
        request.setFields(list);
        
        Record record = getAdapter().retrieve(request);
        String x = "1";
    }
    
    @Test
    public void test_search_submissions_by_form() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Submissions");
        request.setQuery("kapps/services/forms/checkbox-field-bridge-test/submissions");
        
        List<String> list = Arrays.asList("createdBy", "label", "values[Checkbox Field]");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_search_submissions() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Submissions");
        request.setQuery("kapps/services/submissions");
        
        List<String> list = Arrays.asList("createdBy", "label");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_search_submissions_order() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Submissions");
        request.setQuery("kapps/services/submissions?"
                + "timeline=createdAt&direction=ASC");
        
        Map <String, String> metadata = new HashMap<>();
        metadata.put("order", "<%=field[\"createdAt\"]%>:ASC");
        request.setMetadata(metadata);
        
        List<String> list = Arrays.asList("createdAt", "label");
        request.setFields(list);
        
        RecordList records = getAdapter().search(request);
        String x = "1";
    }
    
    @Test
    public void test_count_submissions() throws Exception {
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Submissions");
        request.setQuery("kapps/services/submissions");
        
        List<String> list = Arrays.asList("name", "slug");
        request.setFields(list);
        
        Count count = getAdapter().count(request);
        String x = "1";
    }
    
    @Test
    public void test_index_parse() throws Exception {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("values[Related Id]");
        expectedResult.add("values[Status]");
        
        KineticCoreAdapter adapterClass = new KineticCoreAdapter();
        List<String> result = adapterClass
            .getIndexs("formSlug=milestones&limit=1000&index=values[Related Id],"
                + "values[Status]&q=values[Status]=\"Active\"");
        
        Assert.assertTrue(result.containsAll(expectedResult));
    }
    
        
    @Test
    public void test_index_parse_white_space() throws Exception {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("values[Related Id]");
        expectedResult.add("values[Status]");
        
        KineticCoreAdapter adapterClass = new KineticCoreAdapter();
        List<String> result = adapterClass
            .getIndexs("formSlug=milestones&limit=1000&index=values[Related Id] , "
                + "values[Status]&q=values[Status]=\"Active\"");
        
        Assert.assertTrue(result.containsAll(expectedResult));
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