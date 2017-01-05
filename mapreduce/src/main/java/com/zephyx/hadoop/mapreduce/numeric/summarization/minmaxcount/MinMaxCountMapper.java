package com.zephyx.hadoop.mapreduce.numeric.summarization.minmaxcount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<LongWritable, Text, LongWritable, MinMaxCountTuple>
{

  MinMaxCountTuple post = new MinMaxCountTuple();

  @Override
  protected void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, LongWritable, MinMaxCountTuple>.Context context) throws IOException,
      InterruptedException
  {

    String[] arrUserPost = value.toString().split(",");

    post.setMinPostTime(Long.parseLong(arrUserPost[1]));
    post.setMaxPostTime(Long.parseLong(arrUserPost[1]));
    post.setCount(1L);

    context.write(new LongWritable(Long.parseLong(arrUserPost[0])), post);

  }

}
