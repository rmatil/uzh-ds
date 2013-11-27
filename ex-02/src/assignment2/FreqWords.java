package assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class FreqWords extends Configured implements Tool {

	private static final String INPUT_FILE = "/user/ds2013/data/plot_summaries.txt";
	private static final String TMP_OUTPUT_FOLDER = "/user/ds2013/data/tmpOutput";
	private static final String STOP_WORDS_FILE = "/user/ds2013/stop_words/english_stop_list.txt";
	
	
	public static class Map 
	extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static Set<String> stopWords = new HashSet<String>(); // stopWords read from the english_stop_list.txt-file

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			// text processing and reading of the stop words
			String line = value.toString(); // get line
			line = line.toLowerCase().replace(",", "").replace(".", "").replace("-", " ").replace("\"", "");

			StringTokenizer itr = new StringTokenizer(line); // get each word ("token") of this line
			if (itr.hasMoreTokens()) {
				String movieNumber = itr.nextToken(); // never used
			}
			
			while (itr.hasMoreTokens()) {
				String tmp = itr.nextToken();
				if (!isStopWord(tmp)) {
					word.set(tmp); // Set to contain the contents of a string.
					output.collect(word, one); // Adds a key/value pair to the output
				}
			}
		}

		public void configure(JobConf pJob) {
			// read stop words at startup
			readStopWords();
		}
		
		private static boolean isStopWord(String pWord) {
			if (stopWords.contains(pWord)) {
				return true; // is a stop word
			}
			return false;
		}

		private static void readStopWords() {
			BufferedReader bufferedReader = null;

			try {
				// IOException
				FileSystem fs = FileSystem.get(new Configuration()); // Returns the configured filesystem implementation.
				Path infile = new Path(STOP_WORDS_FILE); // open file with stop words in it
				// IOException
				bufferedReader = new BufferedReader(new InputStreamReader(fs.open(infile))); // creates a Buffered Reader of an InputStream of an FSDataInputStream at the indicated Path.

				// go through the whole stop words file
				// readLine() returns null if end of input stream is reached
				String tmpLine = null;
				while ((tmpLine = bufferedReader.readLine()) != null) { 
					StringTokenizer itr = new StringTokenizer(tmpLine);
					// go through the whole line
					while (itr.hasMoreTokens()) {
						stopWords.add(itr.nextToken()); // add word to stopWords set
					}
				}

				// IOException - If an I/O error occurs
				bufferedReader.close(); // close input stream

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * Reducer
	 */
	public static class Reduce 
	extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			String tmp = "";
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	

	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
		private Text word = new Text();
		private IntWritable one = new IntWritable();

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

			// text processing
			// first: word ,second: lineNumber
			String line = value.toString(); // get line
			line = line.toLowerCase().replace(",", "").replace(".", "").replace("-", " ").replace("\"", "");

			StringTokenizer itr = new StringTokenizer(line); // get each word ("token") of this line

			while (itr.hasMoreTokens()) {
				String tmp = itr.nextToken();
				word.set(tmp); // Set to contain the contents of a string.
				if (itr.hasMoreTokens()) {
					one.set(Integer.parseInt(itr.nextToken()));
				}
				output.collect(one, word); // Adds a key/value pair to the output
			}
		}
	}

	public static class Reduce2 extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(new Text(values.next().toString()), key);
			}
		}
	}

	/*
	 * Sorts the incoming Writables in descending order
	 * This method is just copied out of the hadoop library (IntWritable.Comparator) and modified
	 * to sort in descending order...
	 */
	static class ReverseComparator extends WritableComparator {
		private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

		public ReverseComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return (-1) * INT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof IntWritable && b instanceof IntWritable) {
				return (-1) * (((IntWritable) a).compareTo((IntWritable) b));
			}
			return super.compare(a, b);
		}
	}

	

	public int run(String[] pArgs) throws Exception {
		Configuration config = new Configuration();
		JobConf conf = new JobConf(config, FreqWords.class);

		conf.setJobName("maxword");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// overwrite output path
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(TMP_OUTPUT_FOLDER  + "/part-00000"), true);

		FileInputFormat.setInputPaths(conf, new Path(INPUT_FILE));
		FileOutputFormat.setOutputPath(conf, new Path(TMP_OUTPUT_FOLDER  + "/part-00000"));

		JobClient.runJob(conf);

		// Second job to sort the written output
		Configuration config2 = new Configuration();
		JobConf conf2 = new JobConf(config2, FreqWords.class);

		conf2.setJobName("Max10Words");
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(Map2.class);
		conf2.setReducerClass(Reduce2.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		conf2.setOutputKeyComparatorClass(ReverseComparator.class);

		// overwrite output path
		fs.delete(new Path(pArgs[0]), true);

		FileInputFormat.setInputPaths(conf2, new Path(TMP_OUTPUT_FOLDER  + "/part-00000"));
		FileOutputFormat.setOutputPath(conf2, new Path(pArgs[0]));
		JobClient.runJob(conf2);

		// delete tmp Folder
		fs.delete(new Path(TMP_OUTPUT_FOLDER), true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FreqWords(), args);
	}
}









