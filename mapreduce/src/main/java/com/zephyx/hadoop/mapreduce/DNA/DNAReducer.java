package com.zephyx.hadoop.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public  class DNAReducer 
     extends Reducer<CompositeKey,CompositeValue,CompositeKey, CompositeValue> {

	public void reduce(CompositeKey key, Iterable<CompositeValue> values, 
                     Context context
                     ) throws IOException, InterruptedException {
    for (CompositeValue val : values) {
    	context.write(key, val);
    }
  }
}
