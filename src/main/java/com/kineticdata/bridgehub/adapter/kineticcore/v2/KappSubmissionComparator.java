/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.kineticcore.v2;

import com.kineticdata.bridgehub.adapter.Record;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @author chad.rehm
 */
public class KappSubmissionComparator implements Comparator<Record> {

  private List<String> recordFields;

  public KappSubmissionComparator(List<String> recordFields) {
    this.recordFields = recordFields;
  }

  public int compare(Record r1, Record r2) {
    int result = 0;

    for (String recordField : recordFields) {
      String r1Value = normalize(r1.getValue(recordField).toString());
      String r2Value = normalize(r2.getValue(recordField).toString());
      int fieldComparison = r1Value.compareTo(r2Value);
      if (fieldComparison != 0) {
        result = fieldComparison;
        break;
      }
    }

    return result;
  }

  protected String normalize(String string) {
    String result;
    if (string == null) {
      result = "";
    } else {
      result = string.toLowerCase();
    }
    return result;
  }

}
