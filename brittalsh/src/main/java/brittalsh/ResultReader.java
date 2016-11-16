package brittalsh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.classifier.naivebayes.BayesUtils;

/*
  This class is used to read the output of MapReduceClassifier and 
  transform the categorie_id to people readable professions name by using labelindex.
 */




public class ResultReader {
	
	//generate <ArticleId, categoryId> pair
	public static Map<String, String> readCategoryByArticleIds(Configuration configuration, String ArticleFileName) throws Exception {
		Map<String, String> categoryByArticleIds = new HashMap<String, String>();
		BufferedReader reader = new BufferedReader(new FileReader(ArticleFileName));
		while(true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			String[] tokens = line.split("\t", 2);
			String ArticleId = tokens[0];
			String categoryId = tokens[1];
			categoryByArticleIds.put(ArticleId, categoryId);
		}
		reader.close();
		return categoryByArticleIds;
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("Arguments: [file to be classified] [label index] [MapReduceClassifier's output][ResultReader output path]");
			return;
		}
		//System.out.println("lalala");
		String file = args[3];//output path
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		
		String ArticleFileName = args[0];//file to be classified
		String labelIndexPath = args[1];//label index
		String ArticleCategoryIdsPath = args[2];//MapReduceClassifier's output
		
		Configuration configuration = new Configuration();

		Map<String, String> categoryByArticleIds = readCategoryByArticleIds(configuration, ArticleCategoryIdsPath);
		//using readLabelIndex function to get <category_id, profession's name> pairs
		Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration, new Path(labelIndexPath));

		//for(String key: categoryByArticleIds.keySet())
			//System.out.println(key + " "  + categoryByArticleIds.get(key));
		BufferedReader reader = new BufferedReader(new FileReader(ArticleFileName));
		while(true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			
			
			//System.out.println(line);
			String[] tokens = line.split("\t", 2);
			
			String ArticleId = tokens[0];
			//String xxx = categoryByArticleIds.get(ArticleId);
			//System.out.println(xxx);
			String[] categoryId = categoryByArticleIds.get(ArticleId).split(" ");
			String result = "";
			result = ArticleId + " :  " ;
			//using HashSet to remove duplicated professions
			HashSet<String> unique = new HashSet<String>();
			for (String temp : categoryId)
			{
				unique.add(temp);
			}
			for (String temp2 : unique)
			{
				result = result +  labels.get(Integer.parseInt(temp2))  + ", ";
			}
			result = result + "\n";
			writer.write(result);
		}
		reader.close();
        writer.flush();
        writer.close();
	}
}
