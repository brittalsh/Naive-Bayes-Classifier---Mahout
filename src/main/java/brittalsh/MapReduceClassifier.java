package brittalsh;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This class is used to classify article with model we trained by MapReduce.
 */




public class MapReduceClassifier {

	public static class ClassifierMap extends Mapper<LongWritable, Text, Text, Text> {
		private final static Text outputKey = new Text();
		private final static Text outputValue = new Text();
		private static Classifier classifier;

		@Override
		protected void setup(Context context) throws IOException {
			initClassifier(context);
		}

		private static void initClassifier(Context context) throws IOException {
			if (classifier == null) {
				synchronized (ClassifierMap.class) {
					if (classifier == null) {
						classifier = new Classifier(context.getConfiguration());
					}
				}
			}
		}


		// mapper function is designed to get the three best fit category_ids from Classifier for each input
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("\t", 2);
			if (tokens.length < 2) {
				return;
			}
			String ArticleName = tokens[0];
			String Article = tokens[1];
	
			int[] bestCategoryId = classifier.classify(Article);
			String id = "";
			//create a String contains the three best fit category_ids as output value
			for(int x : bestCategoryId)
			{
				id = id + Integer.toString(x) + " ";
			}
			
			outputValue.set(id);
			outputKey.set(ArticleName);
			context.write(outputKey, outputValue);
		}
	}
	
	


	public static void main(String[] args) throws Exception {
		
		
		if (args.length < 5) {
			System.out.println("Arguments: [model] [dictionnary] [document frequency] [Article file] [output directory]");
			return;
		}
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		
		String modelPath = otherArgs[0];
		String dictionaryPath = otherArgs[1];
		String documentFrequencyPath = otherArgs[2];
		String ArticlesPath = otherArgs[3];
		String outputPath = otherArgs[4];
	
	
		//init the Classifier
		conf.setStrings(Classifier.MODEL_PATH_CONF, modelPath);
		conf.setStrings(Classifier.DICTIONARY_PATH_CONF, dictionaryPath);
		conf.setStrings(Classifier.DOCUMENT_FREQUENCY_PATH_CONF, documentFrequencyPath);
	
		// do not create a new jvm for each task
		conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);
	
		Job job  = Job.getInstance(conf, "classifier");
		
		job.setJarByClass(MapReduceClassifier.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ClassifierMap.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(ArticlesPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
	
		job.waitForCompletion(true);
	}
}
