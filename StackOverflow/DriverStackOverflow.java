import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class DriverStackOverflow extends Configured implements Tool {
	
	public static class JoinMapper extends Mapper<Object, Text, IntWritable, PostTuple> {
		
		private IntWritable outKey = new IntWritable();
		private PostTuple post = new PostTuple();
		private BooleanWritable trueWritable = new BooleanWritable(true);
		private BooleanWritable falseWritable = new BooleanWritable(false);
		private Text tag = new Text();
		private Text nothing = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, PostTuple>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArray = line.split(",");
			int typeId = Integer.parseInt(lineArray[0]);

			if (typeId==1) {//question
				// TODO
				int id =Integer.parseInt(lineArray[1]);
				outKey.set(id);

				String tagString=lineArray[5];
				tag.set(tagString);

				post.setQuestion(trueWritable);
				post.setTag(tag);

				String acceptedAnswer=lineArray[2];
				if(acceptedAnswer.length()==0){
					post.setSolved(falseWritable);
				} else {
					post.setSolved(trueWritable);
				}

			} else if (typeId==2) {//answer
				// TODO
				int parentId=Integer.parseInt(lineArray[3]);
				outKey.set(parentId);
				post.setQuestion(falseWritable);
				post.setSolved(falseWritable);
				post.setTag(nothing);

			}
			if (typeId==1 || typeId==2)	context.write(outKey, post);			
		}		
	}
	
	public static class JoinReducer extends Reducer<IntWritable, PostTuple, Text, IntWritable> {
		private IntWritable outValue = new IntWritable();
		private Text outKey = new Text();
		
		@Override
		protected void reduce(IntWritable key, Iterable<PostTuple> values,
				Reducer<IntWritable, PostTuple, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String tag = "";
			int sum = 0; //the number of answers
			boolean solved = false;

			// TODO
			for(PostTuple post : values){
				if(post.getQuestion().get()) {
					tag=post.getTag().toString();
					solved =post.getSolved().get();
				} else {
					sum++;
				}
			}

			if(solved) {
				outValue.set(-sum);
			} else{
				outValue.set(sum); //for an unsolved question sum may equal 0 
			} 
            outKey.set(tag);
            context.write(outKey, outValue); // (tag; #answers)
		}		
	}

	public static class TagReducer extends Reducer<Text, Text, IntWritable, Text> {
		private Text outValue = new Text();
		private IntWritable outKey = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {

			int solvedCases = 0;
			int unsolvedCases = 0;
			long totalNumberOfAnswersforSolvedCases = 0l;
			long totalNumberOfAnswersforUnsolvedCases = 0l;
			double avgSolved; //the average number of answers per solved question
			double avgUnsolved; //the average number of answers per unsolved question
			// TODO
			for(Text val : values){

				String valString=val.toString();
				int valInt=Integer.parseInt(valString);
				if(valInt>=0) {
					unsolvedCases++;
					totalNumberOfAnswersforUnsolvedCases+=valInt;
				} else {
					solvedCases++;
					totalNumberOfAnswersforSolvedCases+=valInt;
				}
			}
			if(solvedCases!=0){
				avgSolved=(-1)*((double)totalNumberOfAnswersforSolvedCases/(double)solvedCases);
			} else {
				avgSolved= -100;
			}

			if(unsolvedCases!=0){
				avgUnsolved=(double)(totalNumberOfAnswersforUnsolvedCases)/(double)unsolvedCases;
			} else {
				avgUnsolved=-100;
			}

				String summary = String.format("%s \t %d  %.1f  |  %d  %.1f", key.toString(), solvedCases, avgSolved, unsolvedCases, avgUnsolved);
				outValue.set(summary);			
				outKey.set(solvedCases);
				context.write(outKey, outValue);
		}
		
	}
	
	public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
		private IntWritable outKey = new IntWritable();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			int n = Integer.parseInt(key.toString());
			outKey.set(-n);
			context.write(outKey, value);
		}
	}
	
	public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}			
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DriverStackOverflow(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {     
		Configuration conf = getConf();	
		Path tmpPath1 = new Path(args[1]+"Tmp1");
		Path tmpPath2 = new Path(args[1]+"Tmp2");
		
		Job jobA = Job.getInstance(conf, "Programming languages popularity (stage A)");
		jobA.setJarByClass(DriverStackOverflow.class);

		jobA.setMapperClass(JoinMapper.class);
		jobA.setReducerClass(JoinReducer.class);
		jobA.setNumReduceTasks(4);
		
		jobA.setMapOutputKeyClass(IntWritable.class);
		jobA.setMapOutputValueClass(PostTuple.class);
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, tmpPath1);

		int returnValue = jobA.waitForCompletion(true) ? 0 : 1;
		System.out.println("jobA.isSuccessful " + jobA.isSuccessful());
		
		if(returnValue==1) return returnValue; 
		
		Job jobB = Job.getInstance(conf, "Programming languages popularity (stage B)");
		jobB.setJarByClass(DriverStackOverflow.class);
		
		jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		//we use IdentityMapper
		jobB.setReducerClass(TagReducer.class);
		jobB.setNumReduceTasks(4);
		
		jobB.setMapOutputKeyClass(Text.class);
		jobB.setMapOutputValueClass(Text.class);
		jobB.setOutputKeyClass(IntWritable.class);
		jobB.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(jobB, tmpPath1);
		FileOutputFormat.setOutputPath(jobB, tmpPath2);

		returnValue = jobB.waitForCompletion(true) ? 0 : 1;
		System.out.println("jobB.isSuccessful " + jobB.isSuccessful());

		FileSystem fs = FileSystem.get(conf);
		fs.delete(tmpPath1, true);
		if(returnValue==1) return returnValue;
		
		Job jobC = Job.getInstance(conf, "Programming languages popularity (stage C)");
		jobC.setJarByClass(DriverStackOverflow.class);
		
		jobC.setInputFormatClass(KeyValueTextInputFormat.class);
		jobC.setMapperClass(SortMapper.class);
		jobC.setReducerClass(SortReducer.class);
		jobC.setNumReduceTasks(1);
		
		jobC.setMapOutputKeyClass(IntWritable.class);
		jobC.setMapOutputValueClass(Text.class);
		jobC.setOutputKeyClass(Text.class);
		jobC.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(jobC, tmpPath2);
		FileOutputFormat.setOutputPath(jobC, new Path(args[1]));

		returnValue = jobC.waitForCompletion(true) ? 0 : 1;
		System.out.println("jobC.isSuccessful " + jobC.isSuccessful());

		fs.delete(tmpPath2, true);
		
		return returnValue;
	}

}
