package assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class Join extends Configured implements Tool {
	private static final String MOVIE_METADATA_INPUT_FILE = "/user/ds2013/data/movie.metadata.tsv";
	private static final String CHARACTER_METADATA_INPUT_FILE = "/user/ds2013/data/character.metadata.tsv";

	private static Map<Integer, String> actors = new HashMap<Integer, String>();



	/*
	 * reads movieId and movieName
	 */
	public static class Mapper1 
	extends MapReduceBase 
	implements Mapper<LongWritable, Text, IntWritable, Text> {
		private Text word = new Text();

		// gets movie id and movie name
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output,
				Reporter reporter) throws IOException {


			String movieName = "";
			IntWritable movieId = new IntWritable();
			
			String line = value.toString(); // get line

			String[] tokens = line.split("\t");
			// movieId is always at the beginning
			movieId.set(Integer.parseInt(tokens[0]));
			movieName = tokens[2];
			// rest of the line of movie.metadata.tsv not used to complete assignment...


			// output
			word.set(movieName); // Set to contain the contents of a string.
			output.collect(movieId, word); // Adds a key/value pair to the output
		}

		public void configure(JobConf job) {
			// read actors and put them into a map at startup
			BufferedReader bufferedReader = null;
			try {
				// IOException
				FileSystem fs = FileSystem.get(new Configuration()); // Returns the configured filesystem implementation.
				Path infile = new Path(CHARACTER_METADATA_INPUT_FILE); // path for character metadata file
				// IOException
				bufferedReader = new BufferedReader(new InputStreamReader(fs.open(infile))); // creates a Buffered Reader of an InputStream of an FSDataInputStream at the indicated Path.

				// go through the whole stop words file
				// readLine() returns null if end of input stream is reached
				String tmpLine = null;
				while ((tmpLine = bufferedReader.readLine()) != null) { 
					String actorName = "";
					int movieId = 0;
					
					// text processing...
					// Use split with "\t" instead of stringtokenizer...
					String[] tokens = tmpLine.split("\t");
					movieId = Integer.parseInt(tokens[0]);
					if (!tokens[8].contains("/m/") && !tokens[8].matches("^[0-9]{2}.[0-9]{1}")) {
						// it's already the name
						actorName += tokens[8];
					} else {
						// it has to be at position 9
						actorName += tokens[9];
					}
					// add new actor to the map
					if (actors.containsKey(movieId)) {
						String tmpactors = actors.get(movieId);
						tmpactors += ("\t" + actorName);
						// overwrite current value
						actors.put(movieId, tmpactors);
					} else {
						actors.put(movieId, actorName);
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/*
	 * Reducer 1
	 */
	public static class Reducer1 
	extends MapReduceBase 
	implements Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			Text word = new Text();
			int movieId = key.get();
			String movieName = "";
			String actor = "";
			while (values.hasNext()) { // only one
				movieName = values.next().toString();
			}
			//System.out.println("movieName"  + movieName);
			if (actors.containsKey(movieId)) {
				actor = actors.get(movieId);
			}
			//System.out.println("actors: " + actor);

			word.set(movieName + "\t" + actor);
			output.collect(key, word); // delimiter is already a tab, no need to change anything...
		}
	}




	public int run(String[] pArgs) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(pArgs[0]), true);
		Configuration config = new Configuration();
		JobConf conf = new JobConf(config, Join.class);

		conf.setJobName("Join");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Mapper1.class);
		conf.setCombinerClass(Reducer1.class);
		conf.setReducerClass(Reducer1.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		if (pArgs[1] != null) {
			conf.setNumReduceTasks(Integer.parseInt(pArgs[1]));
		}

		// input file of movie_metadata and tmp output folder
		FileInputFormat.setInputPaths(conf, new Path(MOVIE_METADATA_INPUT_FILE));
		FileOutputFormat.setOutputPath(conf, new Path(pArgs[0]));
		// run map-reduce job 1
		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Join(), args);
	}
}
