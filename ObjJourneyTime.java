package org.myorg;
 
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class ObjJourneyTime {
 
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static IntWritable pointId = new IntWritable();	//object class id
    private static IntWritable time = new IntWritable();	//timestamp

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      String file = value.toString();
      String lines[] = file.split("\n");
      DefaultParser parser= new DefaultParser();

      for(int i=0; i< lines.length; i++)
	{
	
	parser.parse(lines[i]);

	if("point".equals(parser.getPointType()))	//point type
	    continue;

        pointId.set(parser.getPointId());
	time.set( parser.getTimestamp());  //timestamp

        output.collect(pointId, time);
      }
    }
  }
 
  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable journey_time = new IntWritable();	 

    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

      int start, stop, tmp;

      start = values.next().get();
      if(values.hasNext())
      {
	stop = values.next().get();
	if(start>stop)
	{
	     tmp = start;
	     start = stop;
	     stop = tmp;	
	}
	journey_time.set( stop - start ); 
     
        output.collect(key, journey_time);
      }
    }
  }
 
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ObjJourneyTime.class);
    conf.setJobName("ObjJourneyTime");
 
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(IntWritable.class);
 
    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setNumReduceTasks(4);
    //conf.setNumMapTasks(4);
 
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
 
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
    JobClient.runJob(conf);
  }
}
