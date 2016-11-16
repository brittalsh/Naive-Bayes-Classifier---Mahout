package brittalsh;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;

/*
 This class is used to create sequence files from the processed training data
 */





public class toSequence {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//Creating sequence files from the processed data
		Configuration conf = new Configuration();
        String inputFileName = args[0];
		String outputDirName = args[1];
		FileSystem fs = FileSystem.get(conf);
		
		//using SequenceFile.Writer to write sequence file
		Writer writer = new SequenceFile.Writer(fs, conf, new Path(outputDirName + "/chunk-0"),
				Text.class, Text.class);
		
		int count = 0;
		BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
		Text key = new Text();
		Text value = new Text();
		while(true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			String[] tokens = line.split("\t", 3);
			if (tokens.length != 3) {
				System.out.println("Skip line: " + line);
				continue;
			}
			//convert the line in form : /profession/article-name message
			String category = tokens[0];
			String id = tokens[1];
			String message = tokens[2];
			key.set("/" + category + "/" + id);
			value.set(message);
			writer.append(key, value);
			count++;
		}
		reader.close();
		writer.close();
		System.out.println("Wrote " + count + " entries."); 

	}

}
