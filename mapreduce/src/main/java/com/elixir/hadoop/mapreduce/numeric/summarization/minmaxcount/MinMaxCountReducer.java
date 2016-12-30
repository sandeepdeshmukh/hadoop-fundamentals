package com.elixir.hadoop.mapreduce.numeric.summarization.minmaxcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends Reducer<LongWritable, MinMaxCountTuple, LongWritable, MinMaxCountTuple>
{
  @Override
  protected void reduce(LongWritable key, Iterable<MinMaxCountTuple> values,
      Reducer<LongWritable, MinMaxCountTuple, LongWritable, MinMaxCountTuple>.Context context) throws IOException,
      InterruptedException
  {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long count = 0L;

    for (MinMaxCountTuple val : values) {
      if (val.getMinPostTime() < min) {
        min = val.getMinPostTime();
      }

      if (val.getMaxPostTime() > max) {
        max = val.getMaxPostTime();
      }

      count = count + val.getCount();

    }
    context.write(key, new MinMaxCountTuple(min, max, count));

  }

}
