package org.myorg;
 
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class ObjClassJourneyTime {
 
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    private static IntWritable objId = new IntWritable();	//object class id
    private static Text mapOutput = new Text();

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      int objClassId, time;

      String file = value.toString();
      String lines[] = file.split("\n");
      DefaultParser parser= new DefaultParser();

      for(int i=0; i< lines.length; i++)
	{
	parser.parse(lines[i]);

	if("point".equals(parser.getPointType()))	//point type
	    continue;

        objId.set(parser.getPointId());
	time = parser.getTimestamp();  //timestamp
	objClassId = parser.getObjClassId();

	mapOutput.set( time + ":" + objClassId );
        output.collect(objId, mapOutput);
      }
    }
  }
 
  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    //private static IntWritable journey_time = new IntWritable();	//point avg speeg 
    private static Text reduceOutput = new Text();

    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

      String temp;
      int start, stop, tmp, objClassId, journey_time;

      //getting time and object class
      temp = values.next().toString();
      String temp2[] = temp.split(":");
      start = Integer.parseInt(temp2[0]);
      objClassId = Integer.parseInt(temp2[1]);

      if(values.hasNext())
      {
		temp = values.next().toString();
        temp2 = temp.split(":");
        stop = Integer.parseInt(temp2[0]);

		if(start>stop)
		{
	     tmp = start;
	     start = stop;
	     stop = tmp;	
		}
		journey_time =  stop - start; 
		reduceOutput.set(objClassId + " " + journey_time); 
        output.collect(key, reduceOutput);
      }
    }
  }


  public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static IntWritable objClassId = new IntWritable();	//object class id
    private static IntWritable time = new IntWritable();	//time
	

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
	tokenizer.nextToken();	//obj Id
	objClassId.set(Integer.parseInt(tokenizer.nextToken()));	//objClassId
	time.set(Integer.parseInt(tokenizer.nextToken()));	//journey time
	output.collect(objClassId, time);
      }
  }
}
 
  public static class Reduce2 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable avg = new IntWritable();	//average journey time

    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

      int sum = 0, count = 0;
      while (values.hasNext()) {
        sum += values.next().get();
	count++;
      }
      avg.set(sum/count);
      output.collect(key, avg);

    }
  }

 
  public static void main(String[] args) throws Exception {

//job1
    JobConf conf = new JobConf(ObjClassJourneyTime.class);
    conf.setJobName("ObjClassJourneyTime1");
 
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);
    
    conf.setNumReduceTasks(3);
    //conf.setNumMapTasks(4);
 
    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
 
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
 
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

//job2 piped to job1
    JobConf conf2 = new JobConf(ObjClassJourneyTime.class);
    conf2.setJobName("ObjClassJourneyTime2");
 
    conf2.setOutputKeyClass(IntWritable.class);
    conf2.setOutputValueClass(IntWritable.class);
    
    //conf2.setNumReduceTasks(3);
    //conf2.setNumMapTasks(4);
 
    conf2.setMapperClass(Map2.class);
    //conf.setCombinerClass(Reduce.class);
    conf2.setReducerClass(Reduce2.class);
 
    conf2.setInputFormat(TextInputFormat.class);
    conf2.setOutputFormat(TextOutputFormat.class);
 
    FileInputFormat.setInputPaths(conf2, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

 
    JobClient.runJob(conf);

    JobClient.runJob(conf2);
  }
}
