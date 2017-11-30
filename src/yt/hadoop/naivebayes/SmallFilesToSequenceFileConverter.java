package yt.hadoop.naivebayes;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSequenceFileConverter extends Configured implements
		Tool {

	// 静态内部类，作为mapper
	static class SequenceFileMapper extends
			Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filenameKey = new Text();

		// setup在task开始前调用，这里主要是初始化filenamekey
		@Override
		protected void setup(Context context) {
			InputSplit split = context.getInputSplit();
			String fileName = ((FileSplit) split).getPath().getName();
			String className = ((FileSplit) split).getPath().getParent().getName();
			filenameKey.set(className + "@" + fileName);
		}

		@Override
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		PrintWriter out = null;
		
		Path inputPath = new Path(conf.get("INPUTPATH"));
		Path outputPath = new Path(conf.get("OUTPUTPATH"));
		
		FileSystem train_file = inputPath.getFileSystem(conf);
		FileStatus[] train_tempList = train_file.listStatus(inputPath);
		String[] INPUT_PATH_TRAIN = new String[train_tempList.length];
		
		boolean flag = true;
		
		for (int i = 0; i < train_tempList.length; i++) {
			INPUT_PATH_TRAIN[i] = train_tempList[i].getPath().toString();
			if(conf.get("INPUTPATH").equals(Utils.BASE_TRAINDATA_PATH)) {
//				Utils.CLASSGROUP += train_tempList[i].getPath().getName() + "/";
				if (flag) {
					out = new PrintWriter(Utils.FILE);
					flag = false;
				}
				out.print(train_tempList[i].getPath().getName() + "/");
			}
			System.out.println("文     件：" + INPUT_PATH_TRAIN[i]);
//			 System.out.println("CLASSGROUP: "+ CLASSGROUP);
		}
		
		
//		Path outputPath = new Path(Utils.SEQUENCE_INPUT_TRAIN_DATA);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf, "SmallFilesToSequenceFileConverter");
		
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);
		job.setMapperClass(SequenceFileMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 根据输入的训练集路径参数，来指定输入
		if (null != INPUT_PATH_TRAIN) {
			for (String path : INPUT_PATH_TRAIN) {
				WholeFileInputFormat.addInputPath(job, new Path(path));
			}
		}
//		WholeFileInputFormat.addInputPath(job, new Path(Utils.BASE_TRAINDATA_PATH));
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		
		if (!flag)
			out.close();

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SmallFilesToSequenceFileConverter(),
				args);
		
		System.exit(exitCode);
	}
}
