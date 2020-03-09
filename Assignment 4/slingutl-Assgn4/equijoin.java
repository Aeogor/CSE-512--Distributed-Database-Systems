/*  Srinivas Lingutla
    CSE 512 - Assignment 4

    Resources: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0

*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class equijoin {
    public static class equiJoinMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String[] splitRow = word.toString().split(",");
                Text text = new Text(splitRow[1]);
                Text longw = new Text(text);
                context.write(longw, word);
            }

        }
    }

    public static class equijoinReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
            ArrayList<String> tab1 = new ArrayList<String>();
            ArrayList<String> tab2 = new ArrayList<String>();
            for (Text v : values) {
                String _v = v.toString();
                String[] vals = _v.split(",");
                String val = vals[0];
		if(tab1.isEmpty())  tab1.add(_v);
		else {
                    String firstElem = tab1.get(0);
                    String[] firstElemsplit = firstElem.split(",");
                    String initial = firstElemsplit[0];

		    if(initial.equals(val)) tab1.add(_v);
		    else if (!initial.equals(val)) tab2.add(_v);

		}
            }
            for(String _tab1 : tab1) {
                for(String _tab2 : tab2) {
                    String tab1String = _tab1.toString();
                    String tab2String = _tab2.toString();
                    result.set(tab2String + ", " + tab1String);
                    context.write(null, result);
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "equijoin");
        job.setJarByClass(equijoin.class);
        job.setMapperClass(equiJoinMapper.class);
        job.setReducerClass(equijoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
