import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import javafx.util.Pair;

// >>> Don't Change
public class TopMeetings extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopMeetings(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        String temporaryPath = conf.get("tmpPath");
        Path tmpPath = new Path(temporaryPath);
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Visitor-visitee combinations");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(CombinationsMap.class);
        jobA.setReducerClass(CombinationsReduce.class);
	    jobA.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopMeetings.class);
        boolean result = jobA.waitForCompletion(true);

	if(result) {
          Job jobB = Job.getInstance(conf, "Top Meetings");
          jobB.setOutputKeyClass(Text.class);
          jobB.setOutputValueClass(IntWritable.class);

          jobB.setMapOutputKeyClass(Text.class);
          jobB.setMapOutputValueClass(IntWritable.class);

          jobB.setMapperClass(TopMeetingsMap.class);
          jobB.setReducerClass(TopMeetingsReduce.class);
          jobB.setNumReduceTasks(1);

          FileInputFormat.setInputPaths(jobB, tmpPath);
          FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

          jobB.setInputFormatClass(KeyValueTextInputFormat.class);
          jobB.setOutputFormatClass(TextOutputFormat.class);

          jobB.setJarByClass(TopMeetings.class);
	  result = jobB.waitForCompletion(true);
        }
        return result ? 0 : 1;
    }
   

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }


    public static class CombinationsMap extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text outValue= new Text();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            outValue.set(String.format("%s %s %s; %s %s", fields[0],fields[1],fields[2],fields[19],fields[20]));
            context.write(outValue, new IntWritable(1));
        }
    }

    public static class CombinationsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int sum=0;
            for (IntWritable val: values) {
        	sum += val.get();
    	    }
    	    context.write(key, new IntWritable(sum));
        }
    }

    public static class TopMeetingsMap extends Mapper<Text, Text, Text, IntWritable> {
        Integer N;         
        private TreeSet<ComparablePair<Integer, String>> countToTitleMap = new TreeSet<ComparablePair<Integer, String>>();
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();


        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            countToTitleMap.add(new ComparablePair<Integer, String>(Integer.parseInt(value.toString()), key.toString()));
            if (countToTitleMap.size() > N){
                countToTitleMap.remove(countToTitleMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // TODO
            for (ComparablePair<Integer, String> pair : countToTitleMap){
                context.write(new Text(pair.getValue()), new IntWritable(pair.getKey()));
            
            }
        }
    }

    public static class TopMeetingsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        Integer N;
        // TODO
        private TreeSet<ComparablePair<Integer, String>> countToTitleMap = new TreeSet<ComparablePair<Integer, String>>();
         private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();

        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // TODO
            int value=0;
            for(IntWritable v : values){
                    value += v.get();
            }

         countToTitleMap.add(new ComparablePair<Integer, String>(value, key.toString()));
            if (countToTitleMap.size() > N){
                countToTitleMap.remove(countToTitleMap.first());
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        // TODO
            for (ComparablePair<Integer, String> pair : countToTitleMap){
                context.write(new Text(pair.getValue()), new IntWritable(pair.getKey()));
            
            }
        }
    }

}

// >>> Don't Change
class ComparablePair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        extends Pair<A,B> 
        implements Comparable<ComparablePair<A, B>>{

    public ComparablePair(A key, B value) {
	super(key, value);
    }

    @Override
    public int compareTo(ComparablePair<A, B> o) {
        int cmp = o == null ? 1 : (this.getKey()).compareTo(o.getKey());
        return cmp == 0 ? (this.getValue()).compareTo(o.getValue()) : cmp;
    }

}
// <<< Don't Change