package com.zephyx.hadoop.mapreduce.numeric.summarization.average;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<LongWritable, CountAverageTuple, LongWritable, CountAverageTuple>
{
  @Override
  protected void reduce(LongWritable key, Iterable<CountAverageTuple> values,
      Reducer<LongWritable, CountAverageTuple, LongWritable, CountAverageTuple>.Context context) throws IOException,
      InterruptedException
  {
    CountAverageTuple result = new CountAverageTuple();
    float sum = 0;
    long count = 0;
    for (CountAverageTuple val : values) {
      count += val.count;
      sum += val.avg * val.count;
    }
    result.setCount(count);
    result.setSum(sum/count);
    context.write(key, result);
  }
}
