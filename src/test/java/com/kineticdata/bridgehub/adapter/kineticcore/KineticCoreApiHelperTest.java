/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
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
}
