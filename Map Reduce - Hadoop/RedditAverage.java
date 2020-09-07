import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable> {

		private static long commentcnt = 1;
		
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			
			LongPairWritable pairout = new LongPairWritable();
		 	
			JSONObject record = new JSONObject(value.toString());

			String RedditName = (String) record.get("subreddit");			 
			long RedditScore = record.getLong("score");						
	 
			Text RedditMap = new Text(RedditName); 
			pairout.set(commentcnt, RedditScore);			
  
			context.write(RedditMap, pairout);			
				
		}
	}
	
	public static class RedditScoreCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable result = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			
			long sumscore = 0;
			long sumcount = 0;

			for (LongPairWritable val : values) {
				sumcount += val.get_0();
				sumscore += val.get_1();
			}
//			System.out.println(sumcount);
//			System.out.println(sumscore);
			result.set(sumcount,sumscore);
			context.write(key, result);
		}
	}

	public static class RedditScoreReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable finalresult = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
		
		long finsumscore = 0;
		long finsumcount = 0;
		double AvgScore = 0;


			for (LongPairWritable val : values) {
				finsumcount += val.get_0();
				finsumscore += val.get_1();
			}
//			System.out.println(finsumscore);
//			System.out.println(finsumcount);
			AvgScore = (double) finsumscore / finsumcount;
			System.out.println(AvgScore);
			finalresult.set(AvgScore);
			context.write(key, finalresult);
		}
	}


	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(RedditScoreCombiner.class);
		job.setReducerClass(RedditScoreReducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);



		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
