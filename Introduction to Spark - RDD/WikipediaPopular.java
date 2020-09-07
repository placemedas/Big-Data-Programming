import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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


public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable> {

		private static long pagecntr = 1;
//		LongPairWritable pairin = new LongPairWritable();
//		LongPairWritable pairout = new LongPairWritable();
		LongWritable pagecnt = new LongWritable();

		private Text datetitle = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
						
			StringTokenizer itr = new StringTokenizer(value.toString());
			String datetime = new String();
			String pagelang = new String();
			String pagename = new String();
			long pagecount = 0;

			int tokenarr = itr.countTokens();

			for (int i = 0;i < tokenarr; i++) {
				if (i == 1) {	
					datetime = itr.nextToken();
//					System.out.println(datetime);
				} else if ( i == 2) {
					pagelang = itr.nextToken();
//					System.out.println(pagelang);
				} else if (i == 3) {
					pagename = itr.nextToken();
//					System.out.println(pagename);
				} else if (i == 4) {
					pagecount = Long.parseLong(itr.nextToken());
//					System.out.println(pagecount);						
					pagecnt.set(pagecount);
					
				}
			}
//			System.out.println("before check");		

			if (pagelang.equals("en")) {
//				System.out.println("Inside the first check");
				if ((pagename.equals("Main_Page")) || (pagename.startsWith("Special:"))) {
				}
				else {
				datetitle.set(datetime);				
				context.write(datetitle, pagecnt);			
				}
			}
		}
	}
	
	public static class WikipediaCombiner
	extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable numtimes_com = new LongWritable();	

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {

		
		long maxvalue_com = 0;
		long eachval_com = 0;

			Iterator<LongWritable> iter = values.iterator();
			while(iter.hasNext()) {
				eachval_com = iter.next().get();
				if (eachval_com > maxvalue_com) {
					maxvalue_com = eachval_com;
				}
			}
				
			numtimes_com.set(maxvalue_com);
			context.write(key, numtimes_com);
			
		}
	}


	public static class WikipediaReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable numtimes = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
		
		long maxvalue = 0;
		long eachval = 0;

		Iterator<LongWritable> iter = values.iterator();
			while(iter.hasNext()) {
				eachval = iter.next().get();
				if (eachval > maxvalue) {
					maxvalue = eachval;
				}
			}
				
			numtimes.set(maxvalue);
			context.write(key, numtimes);
				
		}
	}


	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(WikipediaCombiner.class);
		job.setReducerClass(WikipediaReducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);



		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
