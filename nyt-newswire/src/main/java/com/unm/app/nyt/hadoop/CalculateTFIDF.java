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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
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
import com.unm.app.nyt.hadoop.CalculateTermFrequency.WordFrequencyInDocsMapper;
import com.unm.app.nyt.hadoop.CalculateTermFrequency.WordFrequencyInDocsReducer;
import com.unm.app.nyt.hadoop.WordCountInEachDoc.WordFrequencyInDocMapper;
import com.unm.app.nyt.hadoop.WordCountInEachDoc.WordFrequencyInDocReducer;

public class CalculateTFIDF extends Configured implements Tool {

	public static class WordsWithTDIDFMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text docWithTermFrequency = new Text();

		public WordsWithTDIDFMapper() {
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = Arrays.asList(StringUtils.split(
					value.toString(), "\t"));
			String wordWithDocument = tokens.get(0);
			word.set(StringUtils.substringBefore(wordWithDocument,
					Constants.AT_SEPARATOR));
			docWithTermFrequency.set(StringUtils.substringAfter(wordWithDocument,
					Constants.AT_SEPARATOR)
					+ Constants.EQUALS_SEPARATOR
					+ tokens.get(1));
			context.write(word, docWithTermFrequency);
		}
	}

	public static class WordsWithTDIDFReducer extends
			Reducer<Text, Text, Text, Text> {

		private Text textKey = new Text();
		private Text textValue = new Text();

		public WordsWithTDIDFReducer() {
		}

		@SuppressWarnings("unused")
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int numberOfDocuments = context.getConfiguration().getInt(
					Constants.NUMBER_OF_DOCUMENTS, 0);
			
			int numberOfDocumentsInWhichWordAppears = 0;
			Map<String, String> documentMap = new HashMap<String, String>();
			for (Text value : values) {
				List<String> tokens = Arrays.asList(StringUtils.split(
						value.toString(), Constants.EQUALS_SEPARATOR));
				if (Integer.valueOf(StringUtils.substringBefore(tokens.get(1),
						Constants.SLASH_SEPARATOR)) > 0) {
					numberOfDocumentsInWhichWordAppears++;
				}
				documentMap.put(tokens.get(0), tokens.get(1));
			}

			for (String document : documentMap.keySet()) {
				String[] wordFrequenceAndTotalWords = StringUtils.split(
						documentMap.get(document), Constants.SLASH_SEPARATOR);

				double tf = Double.valueOf(Double
						.valueOf(wordFrequenceAndTotalWords[0])
						/ Double.valueOf(wordFrequenceAndTotalWords[1]));

				if (numberOfDocumentsInWhichWordAppears == 0) {
					numberOfDocumentsInWhichWordAppears = 1;
				}
				double idf = Math.log10((double) numberOfDocuments
						/ (double) (numberOfDocumentsInWhichWordAppears));

				textKey.set(key + Constants.AT_SEPARATOR + document);
				/*this.textValue.set(numberOfDocuments + "/"
						+ numberOfDocumentsInWhichWordAppears + ", "
						+ wordFrequenceAndTotalWords[0] + "/"
						+ wordFrequenceAndTotalWords[1] + ", " + tf * idf);*/
				
				this.textValue.set(Double.toString(tf*idf));
				
				context.write(textKey, textValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CalculateTFIDF(),
				args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(conf);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path outputPathForWordFrequencyPerDocument = new Path(
				Constants.WORD_COUNT_OUTPUT_PER_DOCUMENT);

		if(fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		
		if (fileSystem.exists(outputPathForWordFrequencyPerDocument)) {
			fileSystem.delete(outputPathForWordFrequencyPerDocument, true);
		}

		Path outputPathForWordFrequencyInAllDocuments = new Path(
				Constants.WORD_COUNT_OUTPUT_IN_ALL_DOCUMENTS);
		if (fileSystem.exists(outputPathForWordFrequencyInAllDocuments)) {
			fileSystem.delete(outputPathForWordFrequencyInAllDocuments, true);
		}
		
		Job job1 = new Job(conf, "WordCountPerDocument");
		job1.setJarByClass(WordCountInEachDoc.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setMapperClass(WordFrequencyInDocMapper.class);
		job1.setReducerClass(WordFrequencyInDocReducer.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1,
				outputPathForWordFrequencyPerDocument);

		job1.waitForCompletion(true);

		long totalNumberOfDocuments = job1.getCounters().findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue();
		conf.setLong(Constants.NUMBER_OF_DOCUMENTS, totalNumberOfDocuments);
		
		Job job2 = new Job(conf, "WordCountInAllDocuments");
		job2.setJarByClass(CalculateTermFrequency.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(WordFrequencyInDocsMapper.class);
		job2.setReducerClass(WordFrequencyInDocsReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2,
				outputPathForWordFrequencyPerDocument);
		FileOutputFormat.setOutputPath(job2,
				outputPathForWordFrequencyInAllDocuments);

		job2.waitForCompletion(true);
		
		Job job3 = new Job(conf, "WordsWithTDFIDF");
		job3.setJarByClass(CalculateTFIDF.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setMapperClass(WordsWithTDIDFMapper.class);
		job3.setReducerClass(WordsWithTDIDFReducer.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3,
				outputPathForWordFrequencyInAllDocuments);
		FileOutputFormat.setOutputPath(job3, outputPath);

		return job3.waitForCompletion(true) ? 0 : 1;
	}
}
