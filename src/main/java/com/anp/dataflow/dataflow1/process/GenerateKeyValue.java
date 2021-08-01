package com.anp.dataflow.dataflow1.process;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class GenerateKeyValue extends DoFn<GenericRecord, KV<String,GenericRecord>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4198181967181779062L;
	
	@ProcessElement
	public void process(@Element GenericRecord inputRecord,OutputReceiver<KV<String,GenericRecord>>ouputReceiver) {
		
		String id=inputRecord.get("id").toString();
		ouputReceiver.output(KV.<String,GenericRecord>of(id,inputRecord));
	}

}
