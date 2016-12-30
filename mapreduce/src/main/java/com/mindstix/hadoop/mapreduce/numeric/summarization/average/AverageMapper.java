package com.mindstix.hadoop.mapreduce.numeric.summarization.average;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, LongWritable, CountAverageTuple>
{

  CountAverageTuple outCountAverage = new CountAverageTuple();

  @Override
  protected void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, LongWritable, CountAverageTuple>.Context context) throws IOException,
      InterruptedException
  {
    outCountAverage.setCount(1);
    outCountAverage.setSum(Integer.parseInt(value.toString()));
    context.write(new LongWritable(1L), outCountAverage);
  }

}
