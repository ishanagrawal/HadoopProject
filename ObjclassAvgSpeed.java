package org.myorg;
 
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class ObjclassAvgSpeed {
 
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    private static IntWritable objId = new IntWritable();	//object class id
    private static DoubleWritable speed = new DoubleWritable();	//point current speed

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
      String file = value.toString();
      String lines[] = file.split("\n");
      DefaultParser parser= new DefaultParser();

      for(int i=0; i< lines.length; i++)
	{
      	parser.parse(lines[i]);
      
	objId.set(parser.getObjClassId());
	speed.set(parser.getSpeed());
	output.collect(objId, speed);
	}
    }
  }
 
  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private static DoubleWritable avg = new DoubleWritable(1);	//point avg speeg 

    public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
      double sum = 0;
      int count = 0;
      while (values.hasNext()) {
        sum += values.next().get();
	count++;
      }
      avg.set(sum/count);
      output.collect(key, avg);
    }
  }
 
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ObjclassAvgSpeed.class);
    conf.setJobName("ObjclassAvgSpeed");
 
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(DoubleWritable.class);
     
    conf.setNumReduceTasks(4);
    //conf.setNumMapTasks(4);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
 
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
 
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
    JobClient.runJob(conf);
  }
}
