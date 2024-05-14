import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.NullWritable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class Ex1 extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Ex1(), args);
        System.exit(res);
    }

    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String percentage = conf.get("percentage");
        System.out.println("percentage=" + percentage);
    
        Job job = Job.getInstance(conf, "filtering and random sampling");
        job.setJarByClass(Ex1.class);
    
        job.setMapperClass(VisitorsMap.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
    
        // If you set the number of reducers to 1 without specifying a reducer class,
        // the MapReduce framework will use a single identity reducer that simply collects the output into a single file.
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("job.isSuccessful= " + job.isSuccessful());
        return returnValue;
    }

    public static class VisitorsMap extends Mapper<Object, Text, NullWritable, Text> {
        private Text outValue= new Text();
        private final static SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
        private Random rands = new Random();
        private Double precentage;
        private NullWritable nullWritable = NullWritable.get(); 
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            String strPrecentage=context.getConfiguration().get("percentage");
            precentage=Double.parseDouble(strPrecentage);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields[6] != null && !fields[6].isEmpty() && fields[11] != null && !fields[11].isEmpty()) {
            try {
                Date arrivalDate = frmtIn.parse(fields[6]);
                Date appointmentDate = frmtIn.parse(fields[11]);
                long minutes = getDateDiff(appointmentDate, arrivalDate);
                //outValue.set(String.format("%s,%s,%s,%s,%s,%d", fields[0],fields[1],fields[2],fields[6],fields[11],minutes));
                String joinedFields = String.join(",", fields);
                outValue.set(joinedFields);
                if (rands.nextDouble() < precentage) context.write(nullWritable, outValue);
                
            }
            catch (ParseException e) {
                //skip
            }
        }
        }

        private long getDateDiff(Date end, Date start) {
            long diffInMillis = end.getTime() - start.getTime();
            TimeUnit tu = TimeUnit.MINUTES;
            return tu.convert(diffInMillis, TimeUnit.MILLISECONDS);
        }

    }
}
   



