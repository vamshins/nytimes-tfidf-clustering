package com.unm.app.nyt.clustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.hsqldb.lib.StringUtil;

public class TfidfClustering {
	public static void main(String args[]) throws Exception {

		ArrayList<NamedVector> DocumentList = new ArrayList<NamedVector>();
		NamedVector documentVector = null;
		String word_url_tfidf;
		String word_url;
		String tfidf;
		BufferedReader br = new BufferedReader(new FileReader(
				args[0].toString()));
		while ((word_url_tfidf = br.readLine()) != null) {
			String[] lineSplit = StringUtil.split(word_url_tfidf, "\t");
			word_url = lineSplit[0];
			tfidf = lineSplit[1];
			System.out.println(word_url + "\t" + tfidf);
			documentVector = new NamedVector(new DenseVector(
					new double[] { Double.valueOf(tfidf) }), word_url);
			DocumentList.add(documentVector);
		}

		br.close();

		File testData = new File("WordTfidfClustering/Data");
		if (!testData.exists()) {
			testData.mkdir();
		}
		testData = new File("WordTfidfClustering/Clusters");
		if (!testData.exists()) {
			testData.mkdir();
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("WordTfidfClustering/Data/datafile");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				Text.class, VectorWritable.class);
		VectorWritable vec = new VectorWritable();
		for (NamedVector vector : DocumentList) {
			vec.set(vector);
			writer.append(new Text(vector.getName()), vec);
		}
		writer.close();

		Path clusterpath = new Path("WordTfidfClustering/Clusters/part-00000");
		SequenceFile.Writer writer1 = new SequenceFile.Writer(fs, conf,
				clusterpath, Text.class, Kluster.class);

		int k = 20;

		for (int i = 0; i < k; i++) {
			Vector clustervec = DocumentList.get(i);
			Kluster cluster = new Kluster(clustervec, i,
					new EuclideanDistanceMeasure());
			writer1.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer1.close();

		KMeansDriver.run(conf, new Path("WordTfidfClustering/Data/datafile"),
				new Path("WordTfidfClustering/Clusters/part-00000"), new Path(
						"WordTfidfClustering/Output"), 0.001, 100, true, 0, true);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
				"WordTfidfClustering/Output/" + Cluster.CLUSTERED_POINTS_DIR
						+ "/part-m-0"), conf);

		IntWritable key = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();

		BufferedWriter[] bw = new BufferedWriter[k];
		for (int i = 0; i < k; i++) {
			bw[i] = new BufferedWriter(new FileWriter("Output/SampleOutput-"
					+ i));
		}

		String writeline;

		while (reader.next(key, value)) {
			int clusterNum = Integer.parseInt(key.toString());

			writeline = ((NamedVector) value.getVector()).getName() + "|"
					+ value.getVector().get(0) + "\n";
			System.out.println(writeline);
			bw[clusterNum].write(writeline);
		}
		for (int i = 0; i < 5; i++) {
			bw[i].close();
		}
		reader.close();
	}
}