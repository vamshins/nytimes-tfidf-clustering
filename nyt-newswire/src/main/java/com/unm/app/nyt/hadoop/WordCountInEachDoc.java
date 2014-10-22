package com.unm.app.nyt.hadoop;

/**
 *
 * @author Vamshi Krishna N S
 * Program: WordCountInEachDoc.java
 * Description: Calculates the count of words in all the articles from NYTimes
 * 
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.unm.app.nyt.constants.Constants;
import com.unm.app.stemmer.Stem;

public class WordCountInEachDoc extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountInEachDoc(),
				args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(WordCountInEachDoc.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(WordFrequencyInDocMapper.class);
		job.setReducerClass(WordFrequencyInDocReducer.class);
		FileSystem fileSystem = FileSystem.get(conf);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		if (fileSystem.exists(outputPath)) {
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

	public static class WordFrequencyInDocMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private static final String REGEX_FOR_ALPHA = "[^a-zA-Z ]";

		private final Pattern PATTERN = Pattern.compile("\\w+");

		private IntWritable one = new IntWritable(1);

		public WordFrequencyInDocMapper() {
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			String line = value.toString();
			List<String> paramsList = Arrays.asList(StringUtils.split(line,
					Constants.PARAM_SEPARATOR));

			String newsAbstract = paramsList.get(2);

			if (StringUtils.isNotEmpty(newsAbstract)) {
				newsAbstract = newsAbstract.replaceAll(REGEX_FOR_ALPHA, "");
			}

			List<String> words = Arrays.asList(StringUtils.split(newsAbstract));

			for (String word : words) {
				if (!word.contains(".")) {
					Matcher matcher = PATTERN.matcher(word);

					if (matcher.find()) {
						String matched = matcher.group().toLowerCase();
						if (Constants.STOP_WORDS.contains(matched)
								|| matched.length() < 3) {
							continue;
						}

						try {
							matched = Stem.stemWord(matched);
						} catch (Throwable e) {
							System.out
									.println("Stemming problem for the word \""
											+ matched + "\" @"
											+ paramsList.get(1));
						}
						context.write(new Text(matched + Constants.AT_SEPARATOR
								+ paramsList.get(1)), one);
					}
				}
			}
		}
	}

	public static class WordFrequencyInDocReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public WordFrequencyInDocReducer() {
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable text : values) {
				sum++;
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
