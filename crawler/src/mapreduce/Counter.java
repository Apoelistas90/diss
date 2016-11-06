package mapreduce;

import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

//tf
public class Counter {
	
	public static void main(String[] args) throws Exception{
		String team = args[0];
		String result = args[1];
		
		File folder = new File(team+"/"+result+"/"+"stemmedtweets");
		File[] listOfFiles = folder.listFiles();
		String out = team+"/"+result+"/mapreduce/";
		
		String[] argss = new String[2];
		
	
		for (File file : listOfFiles) {			
			String filePath = folder+"/"+file.getName();
			argss[0] = filePath;
			argss[1] = out + file.getName().substring(0, 3);
			System.out.println(argss[0] + " " +argss[1]);
		  
		}
		
		//WESTHAM/WIN/stemmedtweets/all.csv WESTHAM/WIN/mapreduce/unigrams
			
		for(int i = 0 ;i <1;i++){
			//int res = ToolRunner.run(new Configuration(), new WordCount(), argss);
			//System.exit(res);
		}
		
	}
}
