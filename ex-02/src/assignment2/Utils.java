package assignment2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class Utils {

    public static final String CONF_OUTPUT_SEPARATOR_KEY = "mapred.textoutputformat.separator";
    public static final String STOPWORDS_LOCATION        = "/user/ds2013/stop_words/english_stop_list.txt";


    public static String nullsafeLowercase(String pStr) {
        if (pStr == null) {
            return null;
        }
        return pStr.toLowerCase();
    }

    public static String removeSpecialChars(CharSequence pSeq) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pSeq.length(); i++) {
            char c = pSeq.charAt(i);
            if (c == ',' || c == '.' || c == '"') {
                // do nothing nothing nothing nothing
            } else if (c == '-') {
                sb.append(" ");
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static Set<String> parseStopWords(JobConf pJob) {
        Set<String> ret = new HashSet<String>();
        try {
            FileSystem fs = FileSystem.get(pJob);
            Path p = new Path(STOPWORDS_LOCATION);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
            String word = null;
            while ((word = reader.readLine()) != null) {
                ret.add(word);
            }
            reader.close();
        } catch (Exception pEx) {
            System.err.println("Error in parsing skip lines, please change to the connecting train");
            pEx.printStackTrace();
        }

        return Collections.unmodifiableSet(ret);
    }

}