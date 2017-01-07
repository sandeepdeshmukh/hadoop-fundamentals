package com.zephyx.hadoop.mapreduce.DNA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Writable;

public class CompositeValue implements Writable
{
  String fragment ;
  int chrNo;
  long chrPostion;
  int quality ;

  public CompositeValue(String fragment)
  {
    super();
    this.fragment = fragment;
    Random r = new Random();
    chrNo = r.nextInt(CompositeKey.NUMBER_OF_CHROMOSOMES);
    chrPostion = r.nextInt(CompositeKey.MAX_POS);
    this.quality = r.nextInt(100);
  }

  public CompositeValue()
  {
    // For serialization
  }

  public String getFragment()
  {
    return fragment;
  }

  public void setFragment(String fragment)
  {
    this.fragment = fragment;
  }

  public int getChrNo()
  {
    return chrNo;
  }

  public void setChrNo(int chrNo)
  {
    this.chrNo = chrNo;
  }

  public long getChrPostion()
  {
    return chrPostion;
  }

  public void setChrPostion(long chrPostion)
  {
    this.chrPostion = chrPostion;
  }

  public int getQuality()
  {
    return quality;
  }

  public void setQuality(int quality)
  {
    this.quality = quality;
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(fragment);
    out.writeInt(chrNo);
    out.writeLong(chrPostion);
    out.writeInt(quality);
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    fragment = in.readUTF();
    chrNo = in.readInt();
    chrPostion = in.readLong();
    quality = in.readInt();;
  }
  
  @Override
  public String toString()
  {
    return  chrNo +"," +chrPostion + "," + fragment + "," + quality;
  }

}
