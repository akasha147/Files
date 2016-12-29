import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.nio.channels.FileChannel;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class Replication {
	public static String location=" ";//Add the path to the directory in which the files are present
	
	public  static class Mymapper extends Mapper<Text,BytesWritable,BytesWritable,Text>{
		 @Override
		protected void map(Text key,BytesWritable value,Context context)
				throws IOException, InterruptedException {
			  context.write(value,key);
			 
			 
	      }
		}
	public static class myReducer extends Reducer<BytesWritable,Text,NullWritable,NullWritable > {
		
		@SuppressWarnings("resource")
		@Override
		protected void reduce(BytesWritable key, Iterable<Text> arr,Context cxt)
				throws IOException, InterruptedException {
			int k=0;
			String file_name=null;
			for(Text file:arr){
				file_name=file.toString();
				k++;
				}
			if(k==1)
			{
				
					FileChannel sourceChannel = new FileInputStream(location+file_name).getChannel();
					FileChannel destChannel = new FileOutputStream(location+file_name+"(Copy)").getChannel();
					destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
				
					sourceChannel.close();
					destChannel.close();
				
			}
		}
	
             
		
		}

	
	@SuppressWarnings("deprecation")
	public static void  main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

		
		System.out.println("HEY");
	    Configuration conf=new Configuration();
		Job job=new Job(conf,"Find_Depulicates");
		job.setJarByClass(Replication.class);
		job.setMapperClass(Mymapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(myReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
	     FileInputFormat.addInputPath(job,new Path(args[0]) );
	     FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit ((job.waitForCompletion(true)&&job.waitForCompletion(true))? 0 : 1);
		
		
		
		
	}

}




