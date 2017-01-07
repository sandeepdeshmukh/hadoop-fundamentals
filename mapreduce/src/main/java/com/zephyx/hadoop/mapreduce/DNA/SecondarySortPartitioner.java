package com.zephyx.hadoop.mapreduce.DNA;

import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends
		Partitioner<CompositeKey, CompositeValue> {

	@Override
	public int getPartition(CompositeKey key, CompositeValue value,
			int numPartitions) {
		return key.chrNo % numPartitions;
	}
}
