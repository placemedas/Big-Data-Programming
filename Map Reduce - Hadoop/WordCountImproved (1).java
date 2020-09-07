import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/** imported predefined LongSum-Reduce **/
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
/** imported Java Regex Pattern **/
import java.util.regex.Pattern;

public class WordCountImproved extends Configured implements Tool {
	/** Changed the last argument from IntWritable to LongWritable **/
	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable> {
		/**Create a pattern to check for punctuation and spaces **/
		Pattern word_sep = Pattern.compile("[\\p{Punct}\\s]+");

		/** Changed the constant from intWritable to LongWritable **/
		private final static LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
		/** Commenting String Tokenizer and changing to split line based on patterns**/	
		/*	StringTokenizer itr = new StringTokenizer(value.toString()); */
			String[] itrVal =  word_sep.split(value.toString());
		/*	while(itr.hasMoreTokens()) { */
		/*		word.set(itr.nextToken()); */
			for(String worditr:itrVal) { 		
		/* Below code ensures words with spaces are omitted */
			    if (worditr.length() > 0) {			
		/* Converting all words to lowercase */
				String worditrLow = worditr.toLowerCase();
				word.set(worditrLow);
				context.write(word, one);
			    }
			}
		}
	}



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountImproved.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		/** changed combiner and reducer into LongSumReducer **/
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
