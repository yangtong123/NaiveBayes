package yt.hadoop.naivebayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalcDocNumInClass extends Configured implements Tool {

	public static class CalcDocNumInClassMapper extends
			Mapper<Text, BytesWritable, Text, Text> {
		private Text className = new Text();
		private Text value = new Text();
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			// 获得文件名和文件上级目录名，得到className
			String[] classAndFile = key.toString().split("@"); 
			String className = classAndFile[0];

			this.className.set(className);
			this.value.set("1");
			context.write(this.className, this.value);
		}
	}

	public static class CalcDocNumInClassReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text className = new Text();//类别名
		private Text DocNumInClass = new Text(); // 该类别下文件总数
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text tempvalue : values) {
				String tmp = tempvalue.toString();
				sum += Integer.parseInt(tmp);
			}
			this.className.set(key);
			this.DocNumInClass.set(String.valueOf(sum));
			context.write(this.className, this.DocNumInClass);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = getConf();
		
		Path outputPath = new Path(Utils.DOC_NUM_IN_CLASS_OUTPUT_PATH);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		Job job = Job.getInstance(conf, "CalcDocNumInClass");
		
		job.setJarByClass(CalcDocNumInClass.class);
		job.setMapperClass(CalcDocNumInClassMapper.class);
		job.setCombinerClass(CalcDocNumInClassReducer.class);
		job.setReducerClass(CalcDocNumInClassReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path(Utils.SEQUENCE_INPUT_TRAIN_DATA));
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new CalcDocNumInClass(), args);
		System.exit(res);
	}

}
