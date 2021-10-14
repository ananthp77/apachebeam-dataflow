package com.anp.dataflow.dataflow1.process;

import java.util.ArrayList;
import java.util.List;

import javax.rmi.CORBA.Util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

import com.anp.dataflow.dataflow1.utils.Constants;
import com.anp.dataflow.dataflow1.utils.Utils;

public class TransformData extends DoFn<KV<String,CoGbkResult>, GenericRecord> {
	
	Schema outputSchema;
	
	@Setup
	public void initalizeSchema() {
		try {
			outputSchema=new Schema.Parser().parse(Utils.getSchema(Constants.OUTPUT_SCHEMA));
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	
	
	@ProcessElement
	public void process( @Element KV<String,CoGbkResult>input,OutputReceiver<GenericRecord>outputReceiver) {
		
		String key=input.getKey();
		Iterable<GenericRecord>employeeDataOnKey=input.getValue().getAll(Constants.employeeTag);
		Iterable<GenericRecord>transactionDataOnKey=input.getValue().getAll(Constants.transactionTag);
		ArrayList<GenericRecord>resultdata=new ArrayList<GenericRecord>();
		
		double totalTransaction=0;
		for(GenericRecord employeeRecord:employeeDataOnKey) {
			double salary=(double) employeeRecord.get("salary");
			if(salary>300000) {
				for(GenericRecord studentRecord:transactionDataOnKey) {
					double transactionAmt=(double) studentRecord.get("transaction");
					totalTransaction=totalTransaction+transactionAmt;
				}
				if(totalTransaction<salary) {
					GenericRecord outputRecord=new GenericData.Record(outputSchema);
					outputRecord.put("id", Integer.valueOf(key));
					outputRecord.put("name", employeeRecord.get("name"));
					outputRecord.put("company", employeeRecord.get("name"));
					outputRecord.put("salary", employeeRecord.get("salary"));
					outputRecord.put("totaltransaction", totalTransaction);
					outputReceiver.output(outputRecord);
				}
			}
		}
		
		
		
	}
	

}
