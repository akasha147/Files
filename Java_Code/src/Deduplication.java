import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.File;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class Deduplication {
	public static String location=" ";//Add the path to the directory in which data is present
	
	public  static class Mymapper extends Mapper<Text,BytesWritable,BytesWritable,Text>{
		 @Override
		protected void map(Text key,BytesWritable value,Context context)
				throws IOException, InterruptedException {
			  context.write(value,key);
			 
			 
	      }
		}
	public static class myReducer extends Reducer<BytesWritable,Text,Text,NullWritable > {
		
		@Override
		protected void reduce(BytesWritable key, Iterable<Text> arr,Context cxt)
				throws IOException, InterruptedException {
			int k=0;
			Text image=null;
			for(Text file:arr){
				if(k==0)
				{
				image=file;
				k++;
				}
				else
				{
					File f = new File(location+"/"+file);
					f.delete();
				}
				
			}
			cxt.write(image,NullWritable.get());
             
		}	
		}

	
	@SuppressWarnings("deprecation")
	public static void  main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

		
	    Configuration conf=new Configuration();
		Job job=new Job(conf,"Find_Depulicates");
		job.setJarByClass(Deduplication.class);
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




