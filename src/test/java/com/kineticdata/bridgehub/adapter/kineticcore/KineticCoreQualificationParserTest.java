/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.kineticcore;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
    @Ignore("Write tests")
    public void test_parse() throws Exception {

    }

    @Test
    public void test_parse_ParameterWithBackslash() throws Exception {
        // `\` should be escaped to `\\`

        // Build the parameter map
        Map<String, String> bridgeParameters = new LinkedHashMap<>();
        bridgeParameters.put("widget", "\\");
        String queryString = safely(() -> parser.parse("q=<%=parameter[widget]%>", bridgeParameters));
        assertEquals("q=\"" + "\\\\" + "\"", queryString);
    }

    @Test
    public void test_parse_ParameterWithQuotation() throws Exception {
        // `"` should be escaped to `\"`

        // Build the parameter map
        Map<String, String> bridgeParameters = new LinkedHashMap<>();
        bridgeParameters.put("widget", "\"");
        String queryString = safely(() -> parser.parse("q=<%=parameter[widget]%>", bridgeParameters));
        assertEquals("q=\"" + "\\\"" + "\"", queryString);
    }

    /*----------------------------------------------------------------------------------------------
     * HELPER METHODS
     *--------------------------------------------------------------------------------------------*/

    @FunctionalInterface
    protected interface UnsafeSupplier<T> {
        public T get() throws Exception;
    }

    protected static <T> T safely(UnsafeSupplier<T> supplier) {
        T result;
        try {
            result = supplier.get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getClass().getName() + ": " + e.getMessage(), e);
        }
        return result;
    }
}
