package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import static com.kineticdata.bridgehub.adapter.kineticcore.KineticCoreAdapter.logger;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 */
public class KineticCoreFormHelper {
    private final Pattern attributePattern;

    public KineticCoreFormHelper(String username, String password, String spaceUrl) {
        this.attributePattern = Pattern.compile("(.*?)\\[(.*?)\\]");
    }

    public static final List<String> DETAIL_FIELDS = Arrays.asList(new String[] {
        "createdAt","createdBy","notes","submissionLabelExpression","updatedAt","updatedBy"
    });

    public Count count(BridgeRequest request) throws BridgeError {
        String responce = executeRequest(request);
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray forms = (JSONArray)json.get("forms");

        // Create count object
        return new Count(forms.size());
    }

    public Record retrieve(BridgeRequest request) throws BridgeError {
        String responce = executeRequest(request);
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray forms = (JSONArray)json.get("forms");
        
        JSONObject form;
        // Check if forms or form property was returned.
        if (forms != null) {
            if (forms.size() > 1) {
                throw new BridgeError("Retrieve may only return on Form. "
                    + "Please check query");
            } else {
                form = (JSONObject)forms.get(0);
            }
        } else {
            form = (JSONObject)json.get("form");
        }
        
        return createRecordFromForm(request.getFields(), form);
    }

    public RecordList search(BridgeRequest request) throws BridgeError {
        String responce = executeRequest(request);
        JSONObject json = (JSONObject)JSONValue.parse(responce);
        JSONArray forms = (JSONArray)json.get("forms");

        List<Record> records = createRecordsFromForms(request.getFields(), forms);

        // Add pagination to the returned record list
        int pageToken = request.getMetadata("pageToken") == null 
            || request.getMetadata("pageToken").isEmpty() ? 0 
            : Integer.parseInt(new String(Base64.decodeBase64(
            request.getMetadata("pageToken"))));

        int limit = request.getMetadata("limit") == null 
            || request.getMetadata("limit").isEmpty() ? records.size() - pageToken 
            : Integer.parseInt(request.getMetadata("limit"));

        String nextPageToken = null;
        if (pageToken + limit < records.size()) nextPageToken 
            = Base64.encodeBase64String(
            String.valueOf(pageToken + limit).getBytes());

        records = records.subList(pageToken, pageToken + limit > records.size() 
            ? records.size() : pageToken+limit);

        Map<String,String> metadata = new LinkedHashMap<String,String>();
        metadata.put("size",String.valueOf(limit));
        metadata.put("pageToken",nextPageToken);

        // Return the response
        return new RecordList(request.getFields(), records, metadata);
    }

    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/

    /**
       * Returns the string value of the object.
       * <p>
       * If the value is not a String, a JSON representation of the object will be returned.
       *
       * @param value
       * @return
       */
    private String toString(Object value) {
        String result = null;
        if (value != null) {
            if (String.class.isInstance(value)) {
                result = (String)value;
            } else {
                result = JSONValue.toJSONString(value);
            }
        }
        return result;
     }

    // A helper method used to call createRecordsFromForms but with a
    // single record instead of an array
    private Record createRecordFromForm(List<String> fields, JSONObject form) throws BridgeError {
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(form);
        return createRecordsFromForms(fields,jsonArray).get(0);
    }

    // Made protected and final for the purposes of testing
    protected final List<Record> createRecordsFromForms(List<String> fields, JSONArray forms) throws BridgeError {
        // Go through the users in the JSONArray to create a list of records
        List<Record> records = new ArrayList<Record>();
        for (Object o : forms) {
            JSONObject form = (JSONObject)o;
            Map<String,Object> record = new LinkedHashMap<String,Object>();
            for (String field : fields) {
                Matcher m = this.attributePattern.matcher(field);
                if (m.find()) {
                    record.put(field,toString(getAttributeValues(m.group(1),m.group(2),form)));
                } else {
                    record.put(field,toString(form.get(field)));
                }
            }
            records.add(new Record(record));
        }

        return records;
    }

    private List getAttributeValues(String type, String name, JSONObject form) throws BridgeError {
        if (!form.containsKey(type)) throw new BridgeError(String.format("The field '%s' cannot be found on the Form object",type));
        JSONArray attributes = (JSONArray)form.get(type);
        for (Object attribute : attributes) {
            HashMap attributeMap = (HashMap)attribute;
            if (((String)attributeMap.get("name")).equals(name)) {
                return (List)attributeMap.get("values");
            }
        }
        return new ArrayList(); // Return an empty list if no values were found
    }
    
}
