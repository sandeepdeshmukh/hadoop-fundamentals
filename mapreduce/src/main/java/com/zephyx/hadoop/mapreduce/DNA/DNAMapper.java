package com.zephyx.hadoop.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class DNAMapper 
extends Mapper<Object, Text, CompositeKey, CompositeValue>{

	public void map(Object key, Text value, Context context
	             ) throws IOException,	 InterruptedException {
		
	  CompositeValue dnaValue = new CompositeValue(value.toString());
	  CompositeKey dnaKey = new CompositeKey();
	  dnaKey.setChrNo(dnaValue.getChrNo());
	  dnaKey.setPosition(dnaValue.getChrPostion());
		context.write(dnaKey, dnaValue);
	}
}
