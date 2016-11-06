package util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import au.com.bytecode.opencsv.CSVReader;

public class Try {

	public static void main(String[] args) {
		
		try {
			//C:\Users\Andreas\Documents\My Eclipse\Dissertation\ARSENAL
			String folderPath = "C:/Users/Andreas/Documents/My Eclipse/Dissertation/ARSENAL";
			File folder = new File(folderPath);
			System.out.println(folder.exists());
			File[] listOfFiles = folder.listFiles();
			ArrayList<String> tweetsList = new ArrayList<String>();
			for (File file : listOfFiles) {
			    if (file.isFile()) {
			        System.out.println("Now reading:"+file.getName());
			        
			        String filename = folder+"/"+file.getName();
			        
			        CSVReader reader = new CSVReader(new FileReader(filename));			        
			        String[] line = null;
			   			        
			    
			        
			     // read out header
					line = reader.readNext();
					
					while ((line = reader.readNext())!=null){
						tweetsList.add(line[3]);
					
					}
			    }
			}
			
			String[] tweets = new String[tweetsList.size()];
			for(int i = 0;i<tweetsList.size();i++){
				tweets[i]=tweetsList.get(i);
			}
			System.out.println(tweets.length);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
