package com.anp.dataflow.dataflow1;

import java.io.IOException;
import java.net.URL;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.io.IOUtils;

import com.anp.dataflow.dataflow1.options.CustomOptions;
import com.anp.dataflow.dataflow1.process.StudentEmployeeJoiner;
import com.anp.dataflow.dataflow1.utils.Constants;
import com.anp.dataflow.dataflow1.utils.Utils;
import com.google.common.io.Resources;

/**
 * Hello world!
 *
 */
public class Driver 
{
    public static void main( String[] args ) throws IOException
    {
    	  	
    	
        CustomOptions options=PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);    
    	Pipeline p=Pipeline.create(options);
    	
    	Schema transactionschema=new Schema.Parser().parse(Utils.getSchema(Constants.TRANSACTION_SCHEMA));
    	Schema employeeschema=new Schema.Parser().parse(Utils.getSchema(Constants.EMPLOYEE_SCHEMA));
    	Schema outputShema=new Schema.Parser().parse(Utils.getSchema(Constants.OUTPUT_SCHEMA));
    	
    	String transactionInputFilePath=options.getInputPathTransaction();
    	String employeeInputFilePath=options.getInputPathEmployee();
    	String outputPath=options.getOutputPath();
    
    
    	PCollection<GenericRecord>studentData=p.apply("READ EMPLOYEE DATA",
    			AvroIO.readGenericRecords(employeeschema).from(employeeInputFilePath));
    	
    	PCollection<GenericRecord>employeeData=p.apply("READ TRANSACTION DATA",
    			AvroIO.readGenericRecords(transactionschema).from(transactionInputFilePath));
    	
    	
    	PCollectionList<GenericRecord>studnetAndEmployee=PCollectionList.<GenericRecord>
    			of(studentData).and(employeeData);
    	
    	PCollection<GenericRecord>resultData=studnetAndEmployee.apply("EMP/TRANSACTION JOIN",new StudentEmployeeJoiner())
    			.setCoder(AvroCoder.of(outputShema));
    	
    	resultData.apply("WRITE RESULTS",AvroIO.writeGenericRecords(outputShema).withoutSharding().withSuffix(".avro").to(outputPath));
    	
    	p.run().waitUntilFinish();
    	
    }
}
