import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;



public class Meetings extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Meetings(), args);
        System.exit(res);
    }

     
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "Meetings");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    
        job.setReducerClass(Reduce.class);
        job.setJarByClass(Meetings.class);
        job.setNumReduceTasks(1);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static String readHDFSFile(String path, Configuration conf) throws IOException {
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while ((line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything
                .toString();

    }
    
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text outValue= new Text();
        private Text outKey= new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            outKey.set(String.format("%s %s %s ", fields[1],fields[2],fields[0]));
            outValue.set(String.format("%s", fields[11]));
            context.write(outKey, outValue);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final static SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy hh:mm");
        private Date oldest=null;;
        private Date latest=null;;
        private Text outValue= new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int counter = 0;
        
            try {
                oldest = frmtIn.parse("01/01/3000 00:00");
                latest = frmtIn.parse("01/01/1000 00:00");
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

          for (Text value : values){
            Date date;
            try {
                date = frmtIn.parse(value.toString());
                if(date.before(oldest)){
                    oldest=date;
                }
                if(date.after(latest)){
                    latest=date;
                }
                counter+=1;

            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } 
            
          }
          if (oldest!=null && latest!=null){
            String oldestString= frmtIn.format(oldest);
            String latestString=frmtIn.format(latest);
            outValue.set(String.format("%s\t%s\t%d", oldestString, latestString, counter));

            context.write(key, outValue);
          }
        }
    }

}
