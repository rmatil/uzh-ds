package assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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




public class WordCount extends Configured implements Tool {

	private static final String INPUT_FILE = "/user/ds2013/data/plot_summaries.txt";
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
			line.toLowerCase().replace(",", "").replace(".", "").replace("-", " ").replace("\"", "");

			StringTokenizer itr = new StringTokenizer(line); // get each word ("token") of this line

			while (itr.hasMoreTokens()) {
				String tmp = itr.nextToken();
				if (!isStopWord(tmp) && !isMovieNumber(tmp)) {
					word.set(tmp); // Set to contain the contents of a string.
					output.collect(word, one); // Adds a key/value pair to the output
				}
			}
		}

		public void configure(JobConf pJob) {
			// read stop words at startup
			readStopWords();
		}
		
		private static boolean isMovieNumber(String pWord) {
			Pattern p = Pattern.compile("^[0-9]+"); // at the beginning of the word, one or more than one, numbers
			Matcher m = p.matcher(pWord);
			if (m.find()) {
				return true;
			}
			return false;
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
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(new Text(""), new IntWritable(sum));
		}
	}

	public static void main(String[] pArgs) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), pArgs);
	}

	@Override
	public int run(String[] pArgs) throws Exception {
		// overwrite output path
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(pArgs[0]), true);

		Configuration config = new Configuration();
		JobConf conf = new JobConf(config, WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(INPUT_FILE));
		FileOutputFormat.setOutputPath(conf, new Path(pArgs[0]));

		JobClient.runJob(conf);
		return 0;
	}
}
