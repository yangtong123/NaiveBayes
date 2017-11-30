package yt.hadoop.naivebayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class NaiveBayes extends Configured implements Tool {
	private static Map<String, Double> classPriorProbability = new HashMap<String, Double>(); // 类的先验概率
	private static Map<String, Double> termInClassConditionalProbability = new HashMap<String, Double>(); // 每个单词在类中的条件概率
	private static Map<String, Integer> AllTermNumInClass = new HashMap<String, Integer>(); //每个类中有多少个单词

	public static class NaiveBayesMapper extends Mapper<Text, BytesWritable, Text, Text> {

		private Text docClassAndProbability = new Text();
		// 匹配英文正则表达式
		private static final Pattern PATTERN = Pattern.compile("[/sa-zA-Z]+");
		// 停用词表
		private static String[] stopWordsArray = { "A", "a", "the", "an", "in", "on", "and", "The", "As", "as", "AND" };
		private static Vector<String> stopWords;
		private static String[] classGroup;

		@Override
		protected void setup(Mapper<Text, BytesWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (null == stopWords) {
				stopWords = new Vector<String>();
				for (String tem : stopWordsArray) {
					stopWords.add(tem);
				}
			}

			Configuration conf = context.getConfiguration();
			Path docNumInClass = new Path(Utils.DOC_NUM_IN_CLASS_OUTPUT_PATH + "/part-r-00000");
			Path eachWordNumInClass = new Path(Utils.EACH_WORD_NUM_IN_CLASS_OUTPUT_PATH + "/part-r-00000");
			Path allWordNumInClass = new Path(Utils.ALL_WORD_NUM_IN_CLASS_OUTPUT_PATH + "/part-r-00000");

			classGroup = conf.get("CLASSGROUP").split("/");

			FileSystem fs = FileSystem.get(conf);

			// 从hdfs中读出每个类别的文档数
			SequenceFile.Reader readerOfDocNumInClass = new SequenceFile.Reader(fs, docNumInClass, conf);
			Text keyOfDocNumInClass = new Text();
			Text valueOfDocNumInClass = new Text();
			double sumOfAllDoc = 0;
			Map<String, Integer> tmpClassAndDocNum = new HashMap<String, Integer>();
			while (readerOfDocNumInClass.next(keyOfDocNumInClass, valueOfDocNumInClass)) {
				tmpClassAndDocNum.put(keyOfDocNumInClass.toString(), Integer.parseInt(valueOfDocNumInClass.toString()));
				sumOfAllDoc += Integer.parseInt(valueOfDocNumInClass.toString());
			}
			readerOfDocNumInClass.close();
			// 计算先验概率
			Iterator itOfClassAndDocNum = tmpClassAndDocNum.keySet().iterator();
			while (itOfClassAndDocNum.hasNext()) {
				String classNameKey = (String) itOfClassAndDocNum.next();
				double classPriorValue = tmpClassAndDocNum.get(classNameKey) / sumOfAllDoc;
				System.out.println(classNameKey + "prior probability is " + classPriorValue);
				classPriorProbability.put(classNameKey, classPriorValue);
			}
			// 取出类别中每个单词出现的次数,形式为：ANTA@winning 1
			SequenceFile.Reader readerOfEachWordNumInClass = new SequenceFile.Reader(fs, eachWordNumInClass, conf);
			Text keyOfEachWordNumInClass = new Text();
			IntWritable valueOfEachWordNumInClass = new IntWritable();
			Map<String, Integer> tmpEachTermNumInClass = new HashMap<String, Integer>();
			while (readerOfEachWordNumInClass.next(keyOfEachWordNumInClass, valueOfEachWordNumInClass)) {
				tmpEachTermNumInClass.put(keyOfEachWordNumInClass.toString(), valueOfEachWordNumInClass.get());
			}
			readerOfEachWordNumInClass.close();

			// 取出每个类别中一共有多少个单词term(word),形式为：ANTA 193
			SequenceFile.Reader readerOfAllWordNumInClass = new SequenceFile.Reader(fs, allWordNumInClass, conf);
			Text keyOfAllWordNumInClass = new Text();
			IntWritable valueOfAllWordNumInClass = new IntWritable();
			while (readerOfAllWordNumInClass.next(keyOfAllWordNumInClass, valueOfAllWordNumInClass)) {
				AllTermNumInClass.put(keyOfAllWordNumInClass.toString(), valueOfAllWordNumInClass.get());
			}
			readerOfAllWordNumInClass.close();
			
			//计算条件概率
			Iterator itOfEachTermNumInClass = tmpEachTermNumInClass.keySet().iterator();
			while (itOfEachTermNumInClass.hasNext()) {
				String keyClassAndTerm = "";
				keyClassAndTerm = (String) itOfEachTermNumInClass.next();
				String className = keyClassAndTerm.split("@")[0];
				double valueOfConditionalProbability = (tmpEachTermNumInClass.get(keyClassAndTerm).doubleValue()) 
						/ (AllTermNumInClass.get(className).doubleValue());
				termInClassConditionalProbability.put(keyClassAndTerm, valueOfConditionalProbability);
			}

			super.setup(context);
		}

		@Override
		protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String content = new String(value.getBytes());

			for (String classname : classGroup) {
				Matcher m = PATTERN.matcher(content);
				double multipleTerm = 0;
				while (m.find()) {
					String temkey = m.group();
					String classAndWord;
					if (!stopWords.contains(temkey)) {
						classAndWord = classname + "@" + temkey;
						//如果该单词之前出现过，则取出条件概率
						if (termInClassConditionalProbability.containsKey(classAndWord)) {
							multipleTerm += Math.log10(termInClassConditionalProbability.get(classAndWord));
						} else{//如果该单词是第一次出现，则它的条件概率设为1/all
							multipleTerm += Math.log10(1.0 / AllTermNumInClass.get(classname).doubleValue()) ;
						}
					}
				}
				//再加上先验概率
				multipleTerm += Math.log10(classPriorProbability.get(classname));
				this.docClassAndProbability.set(classname + "/" + Double.toString(multipleTerm));
				System.out.println(key.toString() + "  " + this.docClassAndProbability);
				context.write(key, this.docClassAndProbability);
			}

		}
	}

	public static class NaiveBayesReducer extends Reducer<Text, Text, Text, Text> {
		private Text className = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			double maxProbability = -999999;
			String maxClass = "";

			// 计算文档属于哪一类
			for (Text val : value) {
				String[] tmpVal = val.toString().split("/");
				double probability = Double.valueOf(tmpVal[1]);
				if (probability > maxProbability) {
					maxProbability = probability;
					maxClass = tmpVal[0];
				}
			}
			this.className.set(maxClass + "/" + maxProbability);
			context.write(key, this.className);
			System.out.println(key.toString() + " " + this.className);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path outputPath = new Path(Utils.RESULT_OF_CLASSFICATION);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

//		conf.set("CLASSGROUP", "AFGH/ANTA/USA/CHINA/");

		Job job = Job.getInstance(conf, "NaiveBayes");
		job.setJarByClass(NaiveBayes.class);
		job.setMapperClass(NaiveBayesMapper.class);
		job.setCombinerClass(NaiveBayesReducer.class);
		job.setReducerClass(NaiveBayesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path(Utils.SEQUENCE_INPUT_TEST_DATA));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NaiveBayes(), args);
		System.exit(res);
	}

}
