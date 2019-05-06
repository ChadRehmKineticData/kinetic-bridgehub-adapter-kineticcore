/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.Record;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.simple.parser.JSONParser;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author chad.rehm
 */
public class KineticCoreApiHelperTest {
    
    @Test
    public void test_build_query() throws Exception {
        List<NameValuePair> parameters = new ArrayList<NameValuePair>();
        Set<String> implicitIncludes = new LinkedHashSet<String>();
        KineticCoreApiHelper helper = new KineticCoreApiHelper("user","pass","instance");
       
        parameters.add(new BasicNameValuePair("include", "fields"));
        implicitIncludes.add("details");
        implicitIncludes.add("attributes");
        
        String query = helper.buildQuery(parameters, implicitIncludes);
        assertEquals(query, "include=fields%2Cdetails%2Cattributes");
    }
    
    @Test
    public void test_build_empty_param() throws Exception {
        List<NameValuePair> parameters = new ArrayList<NameValuePair>();
        Set<String> implicitIncludes = new LinkedHashSet<String>();
        KineticCoreApiHelper helper = new KineticCoreApiHelper("user","pass","instance");
       
        implicitIncludes.add("details");
        implicitIncludes.add("attributes");
        
        String query = helper.buildQuery(parameters, implicitIncludes);
        assertEquals(query, "include=details%2Cattributes");
    }    
    
    @Test
    public void test_build_empty_include() throws Exception {
        List<NameValuePair> parameters = new ArrayList<NameValuePair>();
        Set<String> implicitIncludes = new LinkedHashSet<String>();
        KineticCoreApiHelper helper = new KineticCoreApiHelper("user","pass","instance");
       
        parameters.add(new BasicNameValuePair("include", "fields"));
        
        String query = helper.buildQuery(parameters, implicitIncludes);
        assertEquals(query, "include=fields");
    }
    
    
    /*--------------------------------------------------------------------------
    Temp Test for flattenNestedFields
    --------------------------------------------------------------------------*/
    JSONParser parser = new JSONParser();
    String forms = "[{"
        + "\"attributes\":"
            + "[{"
                + "\"name\":\"Icon\","
                + "\"values\":[\"fa-truck\"]},"
                + "{\"name\":\"Owning Team\","
                + "\"values\":[\"Facilities\"]"
            + "}],"
        + "\"name\":\"Cleaning\""
        + "\"slug\":\"cleaning\""
    + "}]";
    
    String submissions = "[{"
        + "\"coreState\":\"Draft\","
        + "\"createdBy\":\"joe.bar@foo.com\","
        + "\"id\":\"3250911c-5afc-11e9-bf69-29dd7c482cf1\","
        + "\"values\":{"
            + "\"Status\":\"Draft\","
            + "\"Requested For\":\"joe.bar@foo.com\""
        + "}"
    + "}]";
    
    String users = "[{"
        + "\"attributesMap\": {"
            + "\"Organization\": ["
                + "\"Architecture & Planning\""
            + "]," 
        + "},"
        + "\"displayName\": \"Aaliyah Wisoky\","
        + "\"email\": \"aaliyah.wisoky@rogahnlarkin.org\","
        + "\"spaceAdmin\": false,"
        + "\"timezone\": null,"
        + "\"username\": \"aaliyah.wisoky@rogahnlarkin.org\""
    + "}]";
    
    @Test
    public void test_form() throws Exception {

        List<String> fields = new ArrayList();
        fields.add("name");
        fields.add("attributes[Icon]");
        fields.add("attributes");
        
        KineticCoreApiHelper helper = new KineticCoreApiHelper("user","pass","instance");
       
        Object obj = parser.parse(forms);
        JSONArray array = (JSONArray)obj;
        
        Map<String,Object> mockRecordMap = new LinkedHashMap<String,Object>();
        mockRecordMap.put("attributes[Icon]", "[\"fa-truck\"]");
        mockRecordMap.put("name", "Cleaning");
        mockRecordMap.put("attributes", "[{"
                + "\"name\":\"Icon\","
                + "\"values\":[\"fa-truck\"]},"
                + "{\"name\":\"Owning Team\","
                + "\"values\":[\"Facilities\"]"
            + "}]");
        Record mockRecord = new Record(mockRecordMap);
        
        List<Record> mockRecords = new ArrayList<Record>();
        mockRecords.add(mockRecord);
        
        List<Record> records = helper.createRecords(fields, array);
        
//        assertEquals(records, mockRecords);
    }
    
    @Test
    public void test_submission() throws Exception {

        List<String> fields = new ArrayList();
        fields.add("id");
        fields.add("values[Status]");
        
        KineticCoreApiHelper helper = new KineticCoreApiHelper("user","pass","instance");
       
        Object obj = parser.parse(submissions);
        JSONArray array = (JSONArray)obj;
        
        Map<String,Object> mockRecordMap = new LinkedHashMap<String,Object>();
        mockRecordMap.put("id", "3250911c-5afc-11e9-bf69-29dd7c482cf1");
        mockRecordMap.put("values[Status]", "Draft");
        Record mockRecord = new Record(mockRecordMap);
        
        List<Record> mockRecords = new ArrayList<Record>();
        mockRecords.add(mockRecord);
        
        List<Record> records = helper.createRecords(fields, array);
        int x = 1;
    }

    @Test
    public void test_user() throws Exception {

        List<String> fields = new ArrayList();
        fields.add("displayName");
        fields.add("attributesMap[Organization]");
        fields.add("spaceAdmin");
        fields.add("attributesMap");
        fields.add("timezone");
        
        KineticCoreApiHelper helper = new KineticCoreApiHelper("user","pass","instance");
       
        Object obj = parser.parse(users);
        JSONArray array = (JSONArray)obj;
        
        Map<String,Object> mockRecordMap = new LinkedHashMap<String,Object>();
        mockRecordMap.put("displayName", "Aaliyah Wisoky");
        mockRecordMap.put("attributesMap[Organization]", "Architecture & Planning");
        mockRecordMap.put("spaceAdmin", "false");
        mockRecordMap.put("attributesMap", "{"
                + "\"Organization\": ["
                    + "\"Architecture & Planning\""
                + "]," 
            + "},");
        mockRecordMap.put("timezone", "null");
        Record mockRecord = new Record(mockRecordMap);
        
        List<Record> mockRecords = new ArrayList<Record>();
        mockRecords.add(mockRecord);
        
        List<Record> records = helper.createRecords(fields, array);
        int x = 1;
    }
}
