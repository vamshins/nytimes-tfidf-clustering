package com.unm.app.nyt.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.unm.app.nyt.constants.Constants;

public class CalculateTermFrequency extends Configured implements Tool {


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CalculateTermFrequency(),
				args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(CalculateTermFrequency.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(WordFrequencyInDocsMapper.class);
		job.setReducerClass(WordFrequencyInDocsReducer.class);
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		if(fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	public static class WordFrequencyInDocsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text document = new Text();
		private Text wordWithCount = new Text();

		public WordFrequencyInDocsMapper() {
		}

		@Override
		protected void map(final LongWritable key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final String line = value.toString();
			final List<String> tokens = Arrays.asList(StringUtils.split(line,
					"\t"));
			System.out.println(tokens);
			final String wordWithFileName = tokens.get(0);
			final String wordCount = tokens.get(1);
			String word = StringUtils.substringBefore(wordWithFileName,
					Constants.AT_SEPARATOR);
			String doc = StringUtils.substringAfter(wordWithFileName,
					Constants.AT_SEPARATOR);
			document.set(doc);
			wordWithCount.set(word + Constants.EQUALS_SEPARATOR + wordCount);
			context.write(document, wordWithCount);
		}
	}

	public static class WordFrequencyInDocsReducer extends
			Reducer<Text, Text, Text, Text> {

		private Text wordAndDocument = new Text();
		private Text wordAverage = new Text();

		public WordFrequencyInDocsReducer() {
		}

		@Override
		@SuppressWarnings("unused")
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int totalWordsInDocument = 0;
			String wordKey = StringUtils.EMPTY;
			final Map<String, String> tempWords = new HashMap<String, String>();
			for (Text text : values) {
				final String word = StringUtils.substringBefore(
						text.toString(), Constants.EQUALS_SEPARATOR);
				final String wordCount = StringUtils.substringAfter(
						text.toString(), Constants.EQUALS_SEPARATOR);
				tempWords.put(word, wordCount);
				totalWordsInDocument = totalWordsInDocument
						+ Integer.valueOf(wordCount);
			}
			for (String word : tempWords.keySet()) {
				String count = tempWords.get(word);
				wordAndDocument.set(word + Constants.AT_SEPARATOR
						+ key.toString());
				wordAverage.set(count + Constants.SLASH_SEPARATOR
						+ totalWordsInDocument);
				context.write(wordAndDocument, wordAverage);
			}
		}
	}
}
