package brittalsh;

import java.io.BufferedReader;
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
 * This class is used to generate test data which appear in lemma_index but not in professions
 */


public class preprocess2 {
	
	public static class Preprocess2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		public static HashMap<String, String> peopleArticlesTitles = new HashMap<String, String>();

    	
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement people articles load from
			// DisßtributedCache here
			super.setup(context);
			//load the professions.txt from HDFS
            URI[] cacheFile = context.getCacheFiles();
            BufferedReader sc = new BufferedReader(new FileReader(cacheFile[0].getPath()));
           
        	//put the names from people.txt into Hashset peopleArticlesTitles 
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
			// TODO: You should implement getting article mapper here
			//input's first word of a row is the article_title, following are word-wordcount pairs
			//if the title does not appear in the peopleArticlesTitles, first get the row and reverse the word count (for example <word, 3> -> word word word)
			//then use every profession as key, row as value
			//for example:  physicist	  Albert Einstein ......
			//                   cosmologist  Albert Einstein ......
			
			//get article-name and find out whether it appears in professions.txt
			String[] splited = inputPage.toString().split("\t", 2);
			if(!peopleArticlesTitles.containsKey(splited[0]))
			{
				String content = "";
				//reverse the process of word count
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
				context.write(new Text(splited[0]), new Text(content));
			}
			
		}
	}
	


	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		

		Job job = Job.getInstance(conf, "GetArticlesMapred"); 
		job.setJarByClass(preprocess2.class);
        //set the distributed cache file's path
		job.addCacheFile(new Path("professions.txt").toUri());    
		
		//set mapper
		
        job.setMapperClass(Preprocess2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //using WikipediaPageInputFormat as input format
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.waitForCompletion(true); 
        
       
	}

}
