import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool{
    
    public static class InitialMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private float initialRank;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) 
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int N = conf.getInt("N", 5706070);
            this.initialRank = (float) 1.0 / N;
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) 
                throws IOException, InterruptedException {
            String line = value.toString(); //A: B C D
            String[] lineArray = value.toString().split(":");
            String page = lineArray[0]; //A
            outKey.set(page);
            String links = "";
            if(lineArray.length == 2) 
                links = lineArray[1].trim();
            outValue.set(initialRank + " " + links); //0.25 B C D
            context.write(outKey, outValue); //A \t 0.25 B C D
        }
    }
    
    public static class SortingMapper extends Mapper<Text, Text, FloatWritable, Text> {
        private FloatWritable pageRank = new FloatWritable();
        

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, FloatWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] rankAndOtherPages = value.toString().split(" ");
            pageRank.set(Float.valueOf(rankAndOtherPages[0]));
            context.write(pageRank, key);
        }
    }

    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        private Text linkTo = new Text();
        private Text contribution = new Text();

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) 
                throws IOException, InterruptedException {
            //A \t 0.25 B C D
            String[] rankAndOtherPages = value.toString().split(" ");
            Float pageRank = Float.valueOf(rankAndOtherPages[0]);
            int arcsOut = rankAndOtherPages.length - 1;
            Float gift = Float.valueOf(pageRank / arcsOut);
            contribution.set(gift.toString());
            for(int i = 1; i <= arcsOut; i++) {
                linkTo.set(rankAndOtherPages[i]);
                context.write(linkTo, contribution); //B \t 0.083333333 
            }
            Text originalRecord = new Text("-" + value.toString()); // a negative pageRank means that this is an original record
            context.write(key, originalRecord); // A \t -0.25 B C D
        } 
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private Text outValue = new Text();
        private float beta = 1;
        private float tax = 0;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.beta = conf.getFloat("B", 1);
            int N = conf.getInt("N", 5706070);
            this.tax = (1 - beta)/N;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) 
                throws IOException, InterruptedException {
            // A \t -0.25 B C D or A \t 0.25
            String originalLinks = "";
            float pageRank = 0f;
            for(Text val : values) {
                String[] rankAndOtherPages = val.toString().split(" ");
                float contribution = Float.parseFloat(rankAndOtherPages[0]);
                if (contribution >= 0) pageRank += contribution;
                else for (int i = 1; i < rankAndOtherPages.length; i++) {
                    originalLinks = originalLinks + " " + rankAndOtherPages[i];
                }
            }
            float pageRankWithTaxation = beta * pageRank + tax;
            outValue.set(pageRankWithTaxation + originalLinks);
            context.write(key, outValue);
        }
    }

    public int run(String[] args) throws Exception {
        NumberFormat nf = new DecimalFormat("00");
        
        Configuration conf = getConf();
        int iterations = conf.getInt("K", 1);
        System.out.println("#iteration=" + iterations);

        String inputPath;
        String outputPath = null;

        Job job = Job.getInstance(conf, "PageRank");
        specifyInitialJob(job, args[0], args[1] + "/00");
        boolean returnValue = job.waitForCompletion(true);

        for (int runs = 0; runs < iterations && returnValue; runs++) {
            job = Job.getInstance(conf, "PageRank");
            inputPath = args[1] + "/" + nf.format(runs);
            outputPath = args[1] + "/" + nf.format(runs + 1);
            specifyIterativeJob(job, inputPath, outputPath);
            returnValue = job.waitForCompletion(true);
            System.out.println("job.isSuccessful=" + job.isSuccessful());
        }

        if (returnValue) {
            job = Job.getInstance(conf, "PageRank");
            specifyFinalJob(job, outputPath, args[1] + "/Final");
            returnValue = job.waitForCompletion(true);
            System.out.println("job.isSuccessful=" + job.isSuccessful());
        }

        return returnValue ? 0 : 1;
    }

    private void specifyInitialJob(Job job, String paramString1, String paramString2) throws IOException {
        job.setJarByClass(PageRank.class);
        job.setMapperClass(InitialMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(paramString1));
        FileOutputFormat.setOutputPath(job, new Path(paramString2));
      }
      
      private void specifyIterativeJob(Job job, String paramString1, String paramString2) throws IOException {
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(paramString1));
        FileOutputFormat.setOutputPath(job, new Path(paramString2));
      }
      
      private void specifyFinalJob(Job job, String paramString1, String paramString2) throws IOException {
        job.setJarByClass(PageRank.class);
        job.setMapperClass(SortingMapper.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(paramString1));
        FileOutputFormat.setOutputPath(job, new Path(paramString2));
      }
      
      public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PageRank(), args);
        System.exit(exitCode);
      }
}        