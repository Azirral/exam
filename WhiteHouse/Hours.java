import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;


public class Hours extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Hours(), args);
        System.exit(res);
    }

    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();    
        Job job = Job.getInstance(conf, "Hours data");
        job.setJarByClass(Hours.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("job.isSuccessful= " + job.isSuccessful());
        return returnValue;
    }

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text outValue= new Text();
        private Text outKey= new Text();
        private final static SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields[6] != null && !fields[6].isEmpty() && fields[11] != null && !fields[11].isEmpty()) {
            try {
                Date arrivalDate = frmtIn.parse(fields[6]);
                Date appointmentDate = frmtIn.parse(fields[11]);
                long minutes = getDateDiff(appointmentDate, arrivalDate);

                Calendar calendar = Calendar.getInstance();
                calendar.setTime(appointmentDate);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);

                if(minutes<=1440){
                    outKey.set(String.format("%02d", hour));
                    outValue.set(String.format("%s,%s,%s", fields[11],fields[6],minutes));

                    context.write(outKey, outValue);
                }
            }
            catch (ParseException e) {
                //skip
            }
        }
        }

        private long getDateDiff(Date end, Date start) {
            long diffInMillis = end.getTime() - start.getTime();
            TimeUnit tu = TimeUnit.MINUTES;
            long diffInMinutes = tu.convert(diffInMillis, TimeUnit.MILLISECONDS);
        
            long diffInDays = TimeUnit.DAYS.convert(diffInMillis, TimeUnit.MILLISECONDS);
        
            if (diffInDays > 0 && diffInMinutes <= 1440) {
                return diffInMinutes;
            } else if (diffInDays == 0) {
                return diffInMinutes;
            }
            return 5000;
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private final static SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
        private Text outValue= new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int counter = 0;
            int delayed=0;
            int ahead=0;
            int aheadMin=0;
            int delayedMin=0;
            int totalAbsError=0;

          for (Text value : values){
            String[] fields = value.toString().split(",");
            Date arrival;
            Date appointment;

            try {
                appointment = frmtIn.parse(fields[0]);
                arrival = frmtIn.parse(fields[1]);
                if(appointment.before(arrival)){
                    delayed+=1;
                    Integer val=Integer.parseInt(fields[2]);
                    delayedMin+=val;
                }   

                if(appointment.after(arrival)){
                    ahead+=1;
                    Integer val1=Integer.parseInt(fields[2]);
                    aheadMin+=val1;
                }
                Integer val=Integer.parseInt(fields[2]);
                totalAbsError+=Math.abs(val);

            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            counter+=1;
                
                
          }
            Double avgDelay=0.0;
            Double avgAhead=0.0;
            Double avgError=0.0;
            if(delayed!=0){
                avgDelay=(double) delayedMin/delayed;
            }
            if(ahead!=0){
                avgAhead=(double) aheadMin/ahead;
            }
            if(counter!=0){
                avgError=(double) totalAbsError/counter;
            }

            outValue.set(String.format("%d, %d, %.1f, %d, %.1f, %.1f", counter, delayed, avgDelay, ahead, avgAhead, avgError));

            context.write(key, outValue);
          }
        }
    }

