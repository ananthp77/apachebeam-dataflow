package com.anp.dataflow.dataflow1.process;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;

import com.anp.dataflow.dataflow1.utils.Constants;
import com.anp.dataflow.dataflow1.utils.Utils;

public class StudentEmployeeJoiner extends PTransform<PCollectionList<GenericRecord>, PCollection<GenericRecord>>{

	@Override
	public PCollection<GenericRecord> expand(PCollectionList<GenericRecord> input) {
		// TODO Auto-generated method stub
		
		PCollection<GenericRecord>studData=input.get(0);
		PCollection<GenericRecord>empData=input.get(0);
		
		try {
			Schema employeechema=new Schema.Parser().parse(Utils.getSchema(Constants.EMPLOYEE_SCHEMA));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	try {
			Schema transactionschema=new Schema.Parser().parse(Utils.getSchema(Constants.TRANSACTION_SCHEMA));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		PCollection<KV<String,GenericRecord>>employeetKV=input.get(0)
				.apply("EMPLOYEE KV MAKER",ParDo.of(new GenerateKeyValue()));
		
		PCollection<KV<String,GenericRecord>>transctionKV=input.get(1)
				.apply("TRANSACTION KV MAKER",ParDo.of(new GenerateKeyValue()));
		
		
		return KeyedPCollectionTuple.of(Constants.employeeTag,employeetKV)
				.and(Constants.transactionTag, transctionKV)
				.apply("GROUP DATA",CoGroupByKey.create())
				.apply("DATA TRNSFORMATION",ParDo.of(new TransformData()));
	}

}
