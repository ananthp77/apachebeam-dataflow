package com.anp.dataflow.dataflow1.options;

import org.apache.beam.sdk.options.PipelineOptions;

public interface CustomOptions extends PipelineOptions{
	
	String getInputPathTransaction();
	
	void setInputPathTransaction(String path);
	
	String getInputPathEmployee();
	
	void setInputPathEmployee(String path);
	
	String getOutputPath();
	
	void setOutputPath(String path);

}
