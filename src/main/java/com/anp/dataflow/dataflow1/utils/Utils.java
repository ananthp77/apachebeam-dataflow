package com.anp.dataflow.dataflow1.utils;

import java.io.IOException;
import java.net.URL;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import com.google.common.io.Resources;

public class Utils {
	
	
    public static String getSchema(String schemaPath) throws IOException {
    	URL schemaVal=null;
    	try {
    		schemaVal=Resources.getResource(schemaPath);
    	}catch(IllegalArgumentException e) {
    		throw new IllegalArgumentException();
    	}
		return new Schema.Parser().parse(IOUtils.toString(schemaVal)).toString();
    }

}
