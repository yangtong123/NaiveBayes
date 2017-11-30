package yt.hadoop.naivebayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {

	public static void main(String[] args) throws Exception {

		// 将训练集中的小文件合并成为sequenceFile
		Configuration confOfTrain = new Configuration();
		confOfTrain.set("INPUTPATH", Utils.BASE_TRAINDATA_PATH);
		confOfTrain.set("OUTPUTPATH", Utils.SEQUENCE_INPUT_TRAIN_DATA);

		SmallFilesToSequenceFileConverter sfToSfc = new SmallFilesToSequenceFileConverter();
		
		
		ToolRunner.run(confOfTrain, sfToSfc, args);

		//计算每个类别中文档数
		CalcDocNumInClass computeDocNumInClass = new CalcDocNumInClass();
		ToolRunner.run(confOfTrain, computeDocNumInClass, args);
		
		//计算每个类中每个单词出现的次数
		CalcEachWordNumInClass computeEachWordNumInClass = new CalcEachWordNumInClass();
		ToolRunner.run(confOfTrain, computeEachWordNumInClass, args);
		
		//计算每个类中的总共单词数
		CalcWordNumInClass computeWordNumInClass = new CalcWordNumInClass();
		ToolRunner.run(confOfTrain, computeWordNumInClass, args);
		
		//统计训练集中所有的单词（不是单词数而是所有的单词）
		CalcAllWordInTrainData computeAllWordInTrainData = new CalcAllWordInTrainData();
		ToolRunner.run(confOfTrain, computeAllWordInTrainData, args);
		
		//将测试集中的所有小文件都转化成sequenceFile
		Configuration confOfTest = new Configuration();
		confOfTest.set("INPUTPATH", Utils.BASE_TESTDATA_PATH);
		confOfTest.set("OUTPUTPATH", Utils.SEQUENCE_INPUT_TEST_DATA);
		
		ToolRunner.run(confOfTest, sfToSfc, args);
		
		//贝叶斯分类
		Configuration confOfBayes = new Configuration();
		NaiveBayes naiveBayes = new NaiveBayes();
		BufferedReader in = new BufferedReader(new FileReader(Utils.FILE));
		String s;
		StringBuilder sb = new StringBuilder();
		while ((s = in.readLine()) != null) {
			sb.append(s);
		}
		in.close();
		System.out.println(sb.toString());
		confOfBayes.set("CLASSGROUP", sb.toString());
		ToolRunner.run(confOfBayes, naiveBayes, args);
	}

}
