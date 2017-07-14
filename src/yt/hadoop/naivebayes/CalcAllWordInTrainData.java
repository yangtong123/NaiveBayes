package yt.hadoop.naivebayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalcAllWordInTrainData extends Configured implements Tool {

	public static class CalcAllWordInTrainDataMapper extends
			Mapper<Text, IntWritable, Text, IntWritable> {
		
		private Text wordID = new Text();
	    private  IntWritable one = new IntWritable(1);
		
		public void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {


			// 获得文件名和文件上级目录名，分别用作classID
			String[] classAndWord = key.toString().split("@");
			
			String word = classAndWord[1];
			
			this.wordID.set(word);		
			context.write(this.wordID, one);
		}
	}

	public static class CalcAllWordInTrainDataReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private  IntWritable one = new IntWritable(1);
		
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			context.write(key,one);

		}

		
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();

		Path outputPath = new Path(Utils.ALL_WORD_IN_TRAIN_OUTPUT_PATH);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

//		Job job = new Job(conf, "CalcWordNumInClass");
		Job job = Job.getInstance(conf, "CalcWordNumInClass");
		
		
		job.setJarByClass(CalcAllWordInTrainData.class);
		job.setMapperClass(CalcAllWordInTrainDataMapper.class);
		job.setCombinerClass(CalcAllWordInTrainDataReducer.class);
		job.setReducerClass(CalcAllWordInTrainDataReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path(Utils.EACH_WORD_NUM_IN_CLASS_OUTPUT_PATH));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CalcAllWordInTrainData(), args);
		System.exit(res);
	}
}