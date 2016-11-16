/**
 * Created by fangwenli on 8/30/16.
 */

import java.io.*;
import java.util.HashMap;

public class evaluate {
    public static void main(String[] args) {
    	
        //read from command line
    	String result = "";
        String pro_70000 = "";
        if (args.length == 2) {
            result = args[0];
            pro_70000 = args[1];
        }

//        boolean firstline = true;
        final HashMap<String, String> peopleArticlesTitles = new HashMap<String, String>();//put title in a hashmap
        String line = "";
        int count = 0;
        int score = 0;

        BufferedReader inputStream1 = null;
        BufferedReader inputStream2 = null;
        try {
            inputStream1 = new BufferedReader(new FileReader(result));
            inputStream2 = new BufferedReader(new FileReader(pro_70000));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
//        BufferedWriter outputStream = null;
//        try {
//            outputStream = new BufferedWriter(new FileWriter(outputPath));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        try {
            	while((line = inputStream2.readLine() ) != null)
        		{
        			String[] pair = line.split(" : ", 2);//split the line, the key is name and professions are value
        			peopleArticlesTitles.put(pair[0], pair[1]);//put them into the hashmap
//        			line = inputStream1.readLine();
        		}

        		
        		while((line = inputStream1.readLine() ) != null)
        		{
        			String[] pair = line.split(" :  ", 2);
        			
        			if(peopleArticlesTitles.containsKey(pair[0])){
        				String[] temps = pair[1].split(", ");
        				for(String temp: temps){
        					if(temp.length()>0){
        						if (peopleArticlesTitles.get(pair[0]).indexOf(temp) != -1)//if one of the professions we predict is in the profession's hashmap
        						{	score ++;//we plus the score we get
        							break;
        						}
        					}
        				}
            			count++;
        			}

//        			line = inputStream1.readLine();
        		}
        		inputStream1.close();
        		inputStream2.close();
        		
        		System.out.println((double)score/(double)count);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

