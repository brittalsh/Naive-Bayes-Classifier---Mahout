package brittalsh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 This class is used to generate training data. Training data should be in the following form:
 		profession1 articlename word1 word2 .......
 		profession2 articlename word1 word2
*/

public class preprocess {
	
	public static class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {
		public static HashMap<String, String> peopleArticlesTitles = new HashMap<String, String>();

    	
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//implement professions.txt load
			// DisßtributedCache here
			super.setup(context);
			//load the people.txt from HDFS
            URI[] cacheFile = context.getCacheFiles();
            BufferedReader sc = new BufferedReader(new FileReader(cacheFile[0].getPath()));
           
        	//put the names and their professions from people.txt into HashMap peopleArticlesTitles 
    		String line=null;
    		while((line = sc.readLine() ) != null)
    		{
    			String[] pair = line.split(" : ", 2);
    			peopleArticlesTitles.put(pair[0], pair[1]);
    		}
    		sc.close();
		}

		@Override
		public void map(LongWritable offset, Text inputPage, Context context)
				throws IOException, InterruptedException {
			//input's first word of a row is the article_title, following are word-wordcount pairs
			//if the title appears in the peopleArticlesTitles, first get the row and reverse the word count (for example <word, 3> -> word word word)
			//then use every profession as key, row as value
			//for example:  physicist	  Albert Einstein ......
			//                   cosmologist  Albert Einstein ......
			
			
			//get article-name and find out whether it appears in professions.txt
			String[] splited = inputPage.toString().split("\t", 2);
			if(peopleArticlesTitles.containsKey(splited[0]))
			{
				//reverse the process of word count
				String[] professions = peopleArticlesTitles.get(splited[0]).split(", ");
				String content = "";
				content = content + splited[0] + "\t";
				String[] pairs = splited[1].split("<|>");
				for (int i = 0; i < pairs.length; )
				{
					if(pairs[i].length() > 2)
					//for the split function may cause a few "", we only need to consider the meaningful splited string
					{
						String num =pairs[i].split(",")[pairs[i].split(",").length-1];
						int count = Integer.parseInt(num);
						num = "," + num;
						String words = pairs[i].replaceAll(num, "");
						for (int j = 0; j< count; j++)
							content = content + words+ " " ;
						i = i + 2;
					}
					else
						i++;
				}
				//for every professsion, generate a profession-article pair
				for(String temp : professions)
					context.write(new Text(temp), new Text(content));
			}
		}
	}
	


	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if (args.length < 3) {
			System.out.println("Arguments: [lemma-index] [outputpath] [professions.txt's path] ");
			return;
		}
		
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		

		Job job = Job.getInstance(conf, "GetArticlesMapred"); 
		job.setJarByClass(preprocess.class);
        //set the distributed cache file's path
		job.addCacheFile(new Path(otherArgs[2]).toUri());    
		
		//set mapper
		
        job.setMapperClass(PreprocessMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.waitForCompletion(true);  
       
       
	}

}
