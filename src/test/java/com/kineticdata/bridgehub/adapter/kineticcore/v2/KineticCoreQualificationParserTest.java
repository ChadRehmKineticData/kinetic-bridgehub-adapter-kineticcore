/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.kineticcore.v2.KineticCoreQualificationParser;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KineticCoreQualificationParserTest {

    protected KineticCoreQualificationParser parser;

    @Before
    public void beforeEach() throws Exception {
        parser = new KineticCoreQualificationParser();
    }

    /*----------------------------------------------------------------------------------------------
     * TESTS
     *--------------------------------------------------------------------------------------------*/

    @Test
    public void test_parsePath() throws Exception {
        String path = parser.parsePath(
            "kapps/services/forms?q=name=*\"c\" AND status=\"Active\""
        );
        
        assertEquals("kapps/services/forms",  path);
    }

    @Test
    public void test_parseQuery() throws Exception {
        
        List<NameValuePair> parameters = parser.parseQuery(
            "kapps/services/forms?q=name=*\"c\" AND status=\"Active\""
        );
        
        // Build the parameter map
        List<NameValuePair> list = new ArrayList<NameValuePair>();
        list.add(new BasicNameValuePair("q", "name=*\"c\" AND status=\"Active\""));
        
        assertEquals(list,  parameters);
    }
    
        @Test
    public void test_parseQuery_multi_param() throws Exception {
        
        List<NameValuePair> parameters = parser.parseQuery(
            "kapps/services/forms?limit=100&q=name=*\"c\" AND status=\"Active\"&"
        );
        
        // Build the parameter map
        List<NameValuePair> list = new ArrayList<NameValuePair>();
        list.add(new BasicNameValuePair("q", "name=*\"c\" AND status=\"Active\""));
        list.add(new BasicNameValuePair("limit", "100"));
        
        assertTrue(list.size() == parameters.size() 
            && list.containsAll(parameters) && parameters.containsAll(list));
    }
    
    @Test
    public void test_parse_ParameterWithBackslash() throws Exception {
        // `\` should be escaped to `\\`

        // Build the parameter map
        Map<String, String> bridgeParameters = new LinkedHashMap<>();
        bridgeParameters.put("widget", "\\");
        String queryString = parser.parse("q=\"<%=parameter[widget]%>\"",
            bridgeParameters);
        
        assertEquals("q=\"" + "\\\\" + "\"", queryString);
    }
    
    @Test
    public void test_parse_ParameterWithBackslashAndQuotation() throws Exception {
        // `\"` should be escaped to `\\\"`

        // Build the parameter map
        Map<String, String> bridgeParameters = new LinkedHashMap<>();
        bridgeParameters.put("widget", "\\\"");
        String queryString = parser.parse("q=\"<%=parameter[widget]%>\"",
            bridgeParameters);
        
        assertEquals("q=\"" + "\\\\\\\"" + "\"", queryString);
    }

    @Test
    public void test_parse_ParameterWithQuotation() throws Exception {
        // `"` should be escaped to `\"`

        // Build the parameter map
        Map<String, String> bridgeParameters = new LinkedHashMap<>();
        bridgeParameters.put("widget", "\"abc");
        String queryString = parser.parse("q=\"<%=parameter[widget]%>\"",
            bridgeParameters);
        
        assertEquals("q=\"" + "\\\"abc" + "\"", queryString);
    }

}
