package com.zephyx.hadoop.mapreduce.numeric.summarization.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable
{

  public CountAverageTuple()
  {
  }

  public CountAverageTuple(long count, float avg)
  {
    this.count = count;
    this.avg = avg;
  }

  public long getCount()
  {
    return count;
  }

  public void setCount(long count)
  {
    this.count = count;
  }

  public float getSum()
  {
    return avg;
  }

  public void setSum(float sum)
  {
    this.avg = sum;
  }

  long count;
  float avg;

  public void write(DataOutput out) throws IOException
  {
    out.writeLong(count);
    out.writeFloat(avg);

  }

  public void readFields(DataInput in) throws IOException
  {
    count = in.readLong();
    avg = in.readFloat();
  }

  @Override
  public String toString()
  {
    return avg + "";
  }

}
