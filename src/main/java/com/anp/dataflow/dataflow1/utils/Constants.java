package com.anp.dataflow.dataflow1.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.TupleTag;

public class Constants {
	
	public static final String TRANSACTION_SCHEMA="transaction.avsc";
	
	public static final String EMPLOYEE_SCHEMA="employee.avsc";
	
	public static final String OUTPUT_SCHEMA="output.avsc";
	
	public static final TupleTag<GenericRecord>employeeTag=new TupleTag<>();
	
	public static final TupleTag<GenericRecord>transactionTag=new TupleTag<>();

}
