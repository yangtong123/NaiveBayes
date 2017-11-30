package yt.hadoop.naivebayes;

public class Utils {
	private static final String BASE_PATH = "hdfs://192.168.56.101:9000/naivebayes/yt/";
//	private static final String BASE_PATH = "/Users/yangtong/Desktop/yt/";
//	private static final String BASE_PATH = "data/yt/";
	
	public static final String BASE_TRAINDATA_PATH = "hdfs://192.168.56.101:9000/naivebayes/yt/training";
//	public static final String BASE_TRAINDATA_PATH = "/Users/yangtong/Desktop/yt/training";
//	public static final String BASE_TRAINDATA_PATH = "data/yt/training";
	
	public static final String BASE_TESTDATA_PATH = "hdfs://192.168.56.101:9000/naivebayes/yt/test";
//	public static final String BASE_TESTDATA_PATH = "/Users/yangtong/Desktop/yt/test";
//	public static final String BASE_TESTDATA_PATH = "data/yt/test";
	
	public static final String SEQUENCE_INPUT_TRAIN_DATA = BASE_PATH + "InputSequenceTrainData";
	
	public static final String SEQUENCE_INPUT_TEST_DATA = BASE_PATH + "InputSequenceTestData";
	
	public static final String DOC_NUM_IN_CLASS_OUTPUT_PATH = BASE_PATH + "DocNumInClass";
	
	public static final String EACH_WORD_NUM_IN_CLASS_OUTPUT_PATH = BASE_PATH + "EachWordNumInClass";
	
	public static final String ALL_WORD_NUM_IN_CLASS_OUTPUT_PATH = BASE_PATH + "AllWordNumInClass";
	
	public static final String ALL_WORD_IN_TRAIN_OUTPUT_PATH = BASE_PATH + "AllWordInTrainData";
	
	public static final String RESULT_OF_CLASSFICATION = BASE_PATH + "ResultOfClassfication";
	
	public static String FILE = "classgroup.txt"; // 输出ClassGroup文件
//	public static String FILE = "data/yt/classgroup.txt"; // 输出ClassGroup文件
}
