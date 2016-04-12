package com.kineticdata.bridgehub.adapter.kineticcore;

import com.kineticdata.bridgehub.adapter.QualificationParser;

/**
 *
 */
public class KineticCoreQualificationParser extends QualificationParser {
    public String encodeParameter(String name, String value) {
        return value;
    }
}