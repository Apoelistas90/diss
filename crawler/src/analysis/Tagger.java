package analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mapreduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.csvreader.CsvWriter;

import au.com.bytecode.opencsv.CSVReader;
import cmu.arktweetnlp.impl.Model;
import cmu.arktweetnlp.impl.ModelSentence;
import cmu.arktweetnlp.impl.Sentence;
import cmu.arktweetnlp.impl.features.FeatureExtractor;


/**
 * Tagger object -- wraps up the entire tagger for easy usage from Java.
 * 
 * To use:
 * 
 * (1) call loadModel().
 * 
 * (2) call tokenizeAndTag() for every tweet.
 *  
 * See main() for example code.
 * 
 * (Note RunTagger.java has a more sophisticated runner.
 * This class is intended to be easiest to use in other applications.)
 */
public class Tagger {
	

	
	public Model model;
	public FeatureExtractor featureExtractor;

	/**
	 * Loads a model from a file.  The tagger should be ready to tag after calling this.
	 * 
	 * @param modelFilename
	 * @throws IOException
	 */
	public void loadModel(String modelFilename) throws IOException {
		model = Model.loadModelFromText(modelFilename);
		featureExtractor = new FeatureExtractor(model, false);
	}

	/**
	 * One token and its tag.
	 **/
	public static class TaggedToken {
		public String token;
		public String tag;
	}


	/**
	 * Run the tokenizer and tagger on one tweet's text.
	 **/
	public List<TaggedToken> tokenizeAndTag(String text) {
		if (model == null) throw new RuntimeException("Must loadModel() first before tagging anything");
		List<String> tokens = Twokenize.tokenizeRawTweetText(text);

		Sentence sentence = new Sentence();
		sentence.tokens = tokens;
		ModelSentence ms = new ModelSentence(sentence.T());
		featureExtractor.computeFeatures(sentence, ms);
		model.greedyDecode(ms, false);

		ArrayList<TaggedToken> taggedTokens = new ArrayList<TaggedToken>();

		for (int t=0; t < sentence.T(); t++) {
			TaggedToken tt = new TaggedToken();
			tt.token = tokens.get(t);
			tt.tag = model.labelVocab.name( ms.labels[t] );
			taggedTokens.add(tt);
		}

		return taggedTokens;
	}
	
	public static String[] getTweets(String folderPath){
		
		String[] tweets = null;
		try {
			
			File folder = new File(folderPath);
			if(folder.exists()){
				System.out.println("Access to "+folder.getAbsolutePath()+" was successfull");
			}
			else{
				System.out.println("Access Denied. Please try again.");
				System.exit(0);
			}
			
			File[] listOfFiles = folder.listFiles();
			ArrayList<String> tweetsList = new ArrayList<String>();

			//Not a folder
			if (listOfFiles == null){
				 CSVReader reader = new CSVReader(new FileReader(folderPath));			        
			        String[] line = null;
			   			     			        
			     // read out header
					line = reader.readNext();
					
					while ((line = reader.readNext())!=null){	
						//NOTE FOR IMPROVEMENT
						tweetsList.add(line[3]);					
					}
			}else{				
				for (File file : listOfFiles) {
				    if (file.isFile()) {
				        System.out.print("Now reading:'"+file.getName()+"'...");
				        
				        String filePath = folder+"/"+file.getName();
				        
				        CSVReader reader = new CSVReader(new FileReader(filePath));			        
				        String[] line = null;
				   			     			        
				     // read out header
						line = reader.readNext();
						
						while ((line = reader.readNext())!=null){
							
							tweetsList.add(line[3]);					
						}
				    }
				  System.out.println("Finished!");
				}
			}
			
			tweets = new String[tweetsList.size()];
			for(int i = 0;i<tweetsList.size();i++){
				tweets[i]=tweetsList.get(i);
			}
			
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return tweets;
		
	}

	
	
	private static Map<List<String>,List<String>> getTweetsAndTheirTokens(Tagger tagger,String team, String result, String matchfile,String operation){
		
		
		Map<List<String>,List<String>> tweetsAndTheirTokens = new HashMap<List<String>,List<String>>();
		String[] tweets = null;
		
		if(operation.equals("mutinfo")){
			/*1. GET ALL TWEETS WITHIN ALL FILES/MATCHES(from a folder)
			 *  This is in order to be able to calculate the Mutual Information Scores.
			 */
			String folderPath = "C:/Users/Andreas/Documents/My Eclipse/Dissertation/"+team+"/"+result+"/input";
			//All files from a pre specified folder.
			tweets = getTweets(folderPath);
		}else if(operation.equals("single")){
			/*2. OR GET TWEETS WITHIN A SINGLE FILE/MATCH
			 * This is in order to be able to construct the training datasets for each instance,
			 * by first POS tagging and stemming. That result is used to construct the vector representation.
			 */
			String filePath = "C:/Users/Andreas/Documents/My Eclipse/Dissertation/"+team+"/"+result+"/input/"+ matchfile;
			tweets = getTweets(filePath);
		}
				
		//Identify what POS tags to keep
		Set<String> posTags = new HashSet<String>();
		posTags.add("A");posTags.add("V");posTags.add("N");posTags.add("R");posTags.add("^");
		posTags.add("M");posTags.add("S");posTags.add("Z");posTags.add("L");posTags.add("!");
		posTags.add("E");
		
		//For each tweet
		for(int i=0;i<tweets.length;i++){
			ArrayList<String> tokens  = new ArrayList<String>();
			ArrayList<String> tags  = new ArrayList<String>();
			String text = tweets[i];			
			//POS Tagging
			
			List<TaggedToken> taggedTokens = tagger.tokenizeAndTag(text);
						
			for (TaggedToken token : taggedTokens) {
							
				if(posTags.contains(token.tag)){
					tokens.add(token.token);
					tags.add(token.tag);
				}
			}			
			tweetsAndTheirTokens.put(tags, tokens);
		}
		return tweetsAndTheirTokens;
	}
	
	//Write to a file. This is because the stemming method requires a file as input.
	private static boolean performStemming(Map<List<String>, List<String>> tweetsAndTheirTokens,String team, String result,String filename) {
		
		//Intermediate Folders
		CsvWriter writer = new CsvWriter(filename);	
		CsvWriter writer2 = new CsvWriter(filename+"_tags");	

		try {
			for(Entry<List<String>,List<String>> entry : tweetsAndTheirTokens.entrySet()){
				List<String> tags = entry.getKey();
				String[] stags = new String[tags.size()];
				
				for(int i=0;i<tags.size();i++){
					stags[i]=tags.get(i);
				}	
				
				List<String> tokens = entry.getValue();	
				String[] stokens = new String[tokens.size()+1];
				//writer.write(tweet);
				for(int i=0;i<tokens.size();i++){
					stokens[i]=tokens.get(i);
				}			
				//This is to identify a new line/tweet
				stokens[tokens.size()] = "*";
				writer.writeRecord(stokens);
				writer2.writeRecord(stags);
			}
			writer.flush();
			writer2.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return stemming(team,result,filename);
	}

	private static boolean stemming(String team,String result,String filename) {		
		 
	      List<String> tokens = new ArrayList<String>();
	      //This is the output destination
	      CsvWriter writer = new CsvWriter("C:/Users/Andreas/Documents/My Eclipse/Dissertation/"+team+"/"+result+"/stemmedtweets/"+filename);
	      Map<Integer, List<String>> stemmedTweetsAndTheirTokens = new HashMap<Integer, List<String>>();
	      
	      int id = 1;
		  char[] w = new char[501];
	      Stemmer s = new Stemmer();
	    
	      for (int i = 0; i < 1; i++)
	      try
	      {
	         FileInputStream in = new FileInputStream(filename);

	         try
	         { 
	           here:
	           while(true){ 
	        	   
	        	  int ch = in.read();
	              if (Character.isLetter((char) ch))
	              {
	            	 
	                 int j = 0;
	                
	                 while(true){ 
	                	 
	                	ch = Character.toLowerCase((char) ch);
	                    w[j] = (char) ch;
	                    if (j < 500) j++;
	                    ch = in.read();
	                    if (!Character.isLetter((char) ch)){
	                       /* to test add(char ch) */
	                     
	                       for (int c = 0; c < j; c++) s.add(w[c]);

	                       /* or, to test add(char[] w, int j) */
	                       /* s.add(w, j); */
	                       
	                       s.stem();
	                       {  
	                    	  String u;

	                          /* and now, to test toString() : */
	                          u = s.toString();
	                         
	                          /* to test getResultBuffer(), getResultLength() : */
	                          /* u = new String(s.getResultBuffer(), 0, s.getResultLength()); */
	                          tokens.add(u);
	                          //System.out.print(u);
	                       }
	                       break;
	                    }
	                 }
	              }
	            
	              if (ch < 0) break;
	             
	              if( (char) ch == '*'){
	            	  if(tokens.size()==0) continue here;
	            	  stemmedTweetsAndTheirTokens.put(id, tokens);
	            	  String[] stokens = new String[tokens.size()];
	            	  for(int si=0;si<tokens.size();si++){
			  				stokens[si]=tokens.get(si);
			  		  }	
	            	 // writer.write(String.valueOf(id));
	            	  writer.writeRecord(stokens);
	            	  tokens = new ArrayList<String>();
	            	  id++;
	              }
	             
	           }
	           writer.flush();
	         }
	         catch (IOException e)
	         {  System.out.println("error reading " + filename);
	            break;
	         }
	      }
	      catch (FileNotFoundException e)
	      {  System.out.println("file " + filename + " not found");
	         break;
	      }
	      return true;
	}

	/**
	 * Illustrate how to load and call the POS tagger.
	 * args[0] = Tagger Input Model(STANDARD)
	 * args[1] = Input team(to access folder)
	 * args[2] = Result of a match - specify either WIN,DRAW,LOSE(to access folder) 
	 * args[3] = Operation to perform. This is either "mutinf"(for all files) or "single"(for a single file)
	 * args[4] = If args[3] = "single" , specify this output as the single match file. If "mutinf" specify this as 'all' files. 
	 * @throws Exception 
	 **/
	public static void main(String[] args) throws Exception {
				
		
			
		String operation = args[3];
		String team = "WESTHAM";//args[1]; ;
		String matchfile =  "wba(0-1)_away.csv";//args[4]; 
		String result = "LOSE";//args[2];	
		
		if (args.length < 1) {
			System.out.println("Supply the model filename as first argument.");
		}
		String modelFilename = args[0];
		
		System.out.println("Model filename for POS Tagging: " + modelFilename);
		Tagger tagger = new Tagger();
		tagger.loadModel(modelFilename);
		
		System.out.println("Getting tweets and doing POS Tagging...");
		Map<List<String>,List<String>> tweetsAndTheirTokens = getTweetsAndTheirTokens(tagger,team,result,matchfile,operation);
		System.out.println("Completed!");
		
		System.out.println("Performing stemming...");
		boolean isComplete =  performStemming(tweetsAndTheirTokens,team,result,matchfile);
		
		if(isComplete){
			System.out.println("Stemming completed!");
			System.out.println("POS Tagging(Stopping) and Stemming are now complete.");
		}
	}
	    	  		
}