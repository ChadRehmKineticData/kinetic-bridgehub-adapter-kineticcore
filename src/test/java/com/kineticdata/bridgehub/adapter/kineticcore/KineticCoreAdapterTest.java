package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Record;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 */
public class KineticCoreAdapterTest extends BridgeAdapterTestBase {

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
        
        Map<String,String> invalidConfiguration = new LinkedHashMap<>();
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
        
        List<String> invalidFields = new ArrayList<>();
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
}
