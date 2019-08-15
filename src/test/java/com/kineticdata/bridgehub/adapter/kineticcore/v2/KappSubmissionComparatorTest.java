package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.kineticcore.v2.KineticCoreAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.Record;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class KappSubmissionComparatorTest extends BridgeAdapterTestBase {
    
    private Map <String, Object> record1Map = new HashMap<String, Object>();
    private Map <String, Object> record2Map = new HashMap<String, Object>();
    private Map <String, Object> record3Map = new HashMap<String, Object>();
    
    private Record record1 = new Record();
    private Record record2 = new Record();
    private Record record3 = new Record();
    
    private List<Record> records = new ArrayList();
    private List<Record> expectedRecords = new ArrayList();
    private List<String> fields = new ArrayList();

    @Override
    public String getConfigFilePath() {
        return "src/test/resources/bridge-config.yml";
    }
    
    @Override
    public Class getAdapterClass() {
        return KineticCoreAdapter.class;
    }
    
    @Test
    public void test_basic_compare() {
        this.record1Map.put("First Name", "chad");
        this.record2Map.put("First Name", "ben");
        this.record3Map.put("First Name", "adam");
        
        this.record1.setRecord(this.record1Map);
        this.record2.setRecord(this.record2Map);
        this.record3.setRecord(this.record3Map);
        
        this.records.add(this.record1);
        this.records.add(this.record2);
        this.records.add(this.record3);
        
        this.expectedRecords.add(this.record3);
        this.expectedRecords.add(this.record2);
        this.expectedRecords.add(this.record1);
        
        
        this.fields.add("First Name");
        
        KappSubmissionComparator comparator = new KappSubmissionComparator(this.fields);
        
        Collections.sort(records, comparator);
        Assert.assertThat(records, IsIterableContainingInOrder.contains(expectedRecords.toArray()));
    }
    
    @Test
    public void test_basic_rev_compare() {
        this.record1Map.put("First Name", "chad");
        this.record2Map.put("First Name", "ben");
        this.record3Map.put("First Name", "adam");
        
        this.record1.setRecord(this.record1Map);
        this.record2.setRecord(this.record2Map);
        this.record3.setRecord(this.record3Map);
        
        this.records.add(this.record2);
        this.records.add(this.record1);
        this.records.add(this.record3);
        
        this.expectedRecords.add(this.record1);
        this.expectedRecords.add(this.record2);
        this.expectedRecords.add(this.record3);
        
        
        this.fields.add("First Name");
        
        KappSubmissionComparator comparator = new KappSubmissionComparator(this.fields);
        
        Collections.sort(records, comparator.reversed());
        Assert.assertThat(records, IsIterableContainingInOrder.contains(expectedRecords.toArray()));
    }
    
    @Test
    public void test_two_compare() {
        this.record1Map.put("First Name", "mike");
        this.record1Map.put("Last Name", "smith");
        this.record2Map.put("First Name", "mike");
        this.record2Map.put("Last Name", "joans");
        this.record3Map.put("First Name", "mike");
        this.record3Map.put("Last Name", "adams");
        
        this.record1.setRecord(this.record1Map);
        this.record2.setRecord(this.record2Map);
        this.record3.setRecord(this.record3Map);
        
        this.records.add(this.record1);
        this.records.add(this.record2);
        this.records.add(this.record3);
        
        this.expectedRecords.add(this.record3);
        this.expectedRecords.add(this.record2);
        this.expectedRecords.add(this.record1);    
        
        this.fields.add("First Name");
        this.fields.add("Last Name");
        
        KappSubmissionComparator comparator = new KappSubmissionComparator(this.fields);
        
        Collections.sort(records, comparator);
        Assert.assertThat(records, IsIterableContainingInOrder.contains(expectedRecords.toArray()));
    }
    
    @Test
    public void test_three_compare() {
        Map <String, Object> record4Map = new HashMap<String, Object>();
        Record record4 = new Record();
        
        this.record1Map.put("State", "MN");
        this.record1Map.put("City", "Appleton");
        this.record1Map.put("Street", "Smith");
        this.record2Map.put("State", "MN");
        this.record2Map.put("City", "Appleton");
        this.record2Map.put("Street", "Jones");
        this.record3Map.put("State", "WI");
        this.record3Map.put("City", "Appleton");
        this.record3Map.put("Street", "Adams");
        record4Map.put("State", "WI");
        record4Map.put("City", "Beaver Dam");
        record4Map.put("Street", "Smith");
        
        this.record1.setRecord(this.record1Map);
        this.record2.setRecord(this.record2Map);
        this.record3.setRecord(this.record3Map);
        record4.setRecord(record4Map);
        
        this.records.add(this.record1);
        this.records.add(this.record2);
        this.records.add(this.record3);
        this.records.add(record4);
        
        this.expectedRecords.add(this.record2);
        this.expectedRecords.add(this.record1);
        this.expectedRecords.add(this.record3);
        this.expectedRecords.add(record4);    
        
        this.fields.add("State");
        this.fields.add("City");
        this.fields.add("Street");
        
        KappSubmissionComparator comparator = new KappSubmissionComparator(this.fields);
        
        Collections.sort(records, comparator);
        Assert.assertThat(records, IsIterableContainingInOrder.contains(expectedRecords.toArray()));
    }
}