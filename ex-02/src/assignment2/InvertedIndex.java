package assignment2;

import java.io.IOException;
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


public class InvertedIndex
        extends Configured
        implements Tool {

    private static final String INPUT_LOCATION = "/user/ds2013/data/plot_summaries.txt";


    public static void main(String[] pArgs)
            throws Exception {
        int res = ToolRunner.run(new Configuration(), new InvertedIndex(), pArgs);
        System.exit(res);
    }


    @Override
    public int run(String[] pArgs)
            throws Exception {
        // Validate CLI args
        int numOfReducers = -1;
        if (pArgs.length < 1) {
            throw new IllegalArgumentException("You need to set the output path.");
        } else if (pArgs.length > 1) {
            try {
                numOfReducers = Integer.parseInt(pArgs[1]);
                if (numOfReducers < 0) {
                    throw new Exception(); // is catched in this block
                }
            } catch (Exception pEx) {
                throw new IllegalArgumentException("Second argument (number of reducers) must be a positive (or 0) integer.");
            }
        }

        JobConf conf = new JobConf(getConf(), InvertedIndex.class);
        conf.setJobName("inverted-index");

        if (numOfReducers >= 0) {
            conf.setNumReduceTasks(numOfReducers);
        }

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MapperClass.class);
        conf.setCombinerClass(SimplerReducerClass.class);
        conf.setReducerClass(SimplerReducerClass.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.set(Utils.CONF_OUTPUT_SEPARATOR_KEY, " ");

        FileInputFormat.addInputPath(conf, new Path(INPUT_LOCATION));
        FileSystem.get(conf).delete(new Path(pArgs[0]), true);
        FileOutputFormat.setOutputPath(conf, new Path(pArgs[0]));

        JobClient.runJob(conf);

        return 0;
    }

    public static class MapperClass
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE        = new IntWritable(1);

        private Set<String>              mStopWords = new HashSet<String>();
        private Text                     mWord      = new Text();
        private String                   mSeparator;

        @Override
        public void map(LongWritable pKey, Text pValue, OutputCollector<Text, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            String line = Utils.removeSpecialChars(Utils.nullsafeLowercase(pValue.toString()));
            StringTokenizer itr = new StringTokenizer(line);

            if (itr.hasMoreTokens()) {
                String movieId = itr.nextToken();
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken();
                    if (!mStopWords.contains(token)) {
                        mWord.set(new StringBuilder(token).append(mSeparator).append(movieId).toString());
                        pOutput.collect(mWord, ONE);
                    }
                }
            }
        }

        @Override
        public void configure(JobConf pJob) {
            mStopWords = Utils.parseStopWords(pJob);
            mSeparator = pJob.get(Utils.CONF_OUTPUT_SEPARATOR_KEY, " ");
        }

    }

    public static class SimplerReducerClass
            extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        public void reduce(Text pKey, Iterator<IntWritable> pValues, OutputCollector<Text, IntWritable> pOutput, Reporter pReporter)
                throws IOException {
            int sum = 0;
            while (pValues.hasNext()) {
                sum += pValues.next().get();
            }
            pOutput.collect(pKey, new IntWritable(sum));
        }

    }

    public static class ReducerClass
            extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, Text> {

        private String mSeparator;

        @Override
        public void reduce(Text pKey, Iterator<IntWritable> pValues, OutputCollector<Text, Text> pOutput, Reporter pReporter)
                throws IOException {
            String[] split = pKey.toString().split(" "); // As this is a reducer only used by InvertedIndexing with its custom Mapper this always returns 2 entries
            Text word = new Text(split[0]);
            String movieId = split[1];
            while (pValues.hasNext()) {
                pOutput.collect(word, new Text(new StringBuilder(movieId).append(mSeparator).append(pValues.next()).toString()));
            }
        }

        @Override
        public void configure(JobConf pJob) {
            mSeparator = pJob.get(Utils.CONF_OUTPUT_SEPARATOR_KEY, " ");
        }

    }

}