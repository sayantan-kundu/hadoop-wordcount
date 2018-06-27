import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WC extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int r = ToolRunner.run(new WC(), args);
		System.exit(r);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Path ip = new Path(arg0[0]);
		Path op = new Path(arg0[1]);
		
		Configuration conf = getConf();
		Job job = new Job(conf);
		
		FileInputFormat.setInputPaths(job, ip);
		FileOutputFormat.setOutputPath(job,op);
		
		job.setJobName("WordCount");
		job.setJarByClass(WC.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapr.class);
		job.setReducerClass(Reducr.class);
		
		int cp = job.waitForCompletion(true)? 0:1;
		return cp;
	}
	
	public static class Mapr extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
			String v = value.toString();
			StringTokenizer st = new StringTokenizer(v);
			Text w = new Text();
			IntWritable one = new IntWritable(1);
			while(st.hasMoreElements()){
				String wr = st.nextToken();
				w.set(wr);
				context.write(w, one);
			}
		};
	}
	
	public static class Reducr extends Reducer<Text, IntWritable, Text, IntWritable>{
		protected void reduce(Text arg0, java.lang.Iterable<IntWritable> arg1, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context arg2) throws java.io.IOException ,InterruptedException {
			int sum=0;
			for(IntWritable i: arg1){
				sum+=i.get();
			}
			arg2.write(arg0, new IntWritable(sum));
		};
	}

}
