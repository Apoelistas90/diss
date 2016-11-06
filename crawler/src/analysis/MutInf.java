package analysis;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.csvreader.CsvWriter;

import au.com.bytecode.opencsv.CSVReader;

public class MutInf {
	

	static double totalWinTweets = 0.0;
	static double totalDrawTweets = 0.0;
	static double totalLoseTweets = 0.0;
	
	static Map<Double,String> results = new TreeMap<Double,String>();
	
	/*
	 * args[0] = All_Win Tweets
	 * args[1] = All_Draw Tweets
	 * args[2] = All_Lose Tweets
	 */
	public static void main(String[] args) {
		
		Map<String,Double> winContents = getTweets(args[0]);
		Map<String,Double> drawContents =  getTweets(args[1]);
		Map<String,Double> loseContents =  getTweets(args[2]);		
		
		int distinctWin = winContents.size();
		int distinctDraw = drawContents.size();
		int distinctLose = loseContents.size();
		
		System.out.println("Total tweets for win occassions: "+totalWinTweets);
		System.out.println("Total tweets for draw occassions: "+totalDrawTweets);
		System.out.println("Total tweets for lose occassions: "+totalLoseTweets);
		
		double totalTweets = totalWinTweets + totalDrawTweets + totalLoseTweets;
		
		System.out.println("Distinct n-grams for win occassions: "+distinctWin);
		System.out.println("Distinct n-grams for draw occassions: "+distinctDraw);
		System.out.println("Distinct n-grams for lose occassions: "+distinctLose);
		
		Double termWinTrue = 0.0;
		Double termWinFalse = 0.0;
		Double termOtherTrue = 0.0;
		Double termOtherFalse = 0.0;
		
		//The following three loops are in order to make up the contigency matrix for each of the three response variables.
		
		System.out.println("Calculating Mutual Information...");
		//Optimize using Sets instead of Maps.
		//CLASS WIN
		for(Entry<String,Double> win : winContents.entrySet()){
			String term = win.getKey();
			termWinTrue = (double) win.getValue();
			termWinFalse = (double) totalWinTweets - termWinTrue;
			//System.out.println(term + " " + termWinTrue + " " + termWinFalse);
			for(Entry<String,Double> draw : drawContents.entrySet()){
				if(term.equals(draw.getKey())){
					termOtherTrue = (double) draw.getValue();
					break;
				}else{
					termOtherTrue += 0.0;
				}
			}
			for(Entry<String,Double> lose : loseContents.entrySet()){
				if(term.equals(lose.getKey())){
					termOtherTrue += (double) lose.getValue();
					break;
				}else{
					termOtherTrue += 0.0;
				}
			}
			termOtherFalse = totalTweets - termOtherTrue;
			results.put(calcMutInf(termWinTrue,termWinFalse,termOtherTrue,termOtherFalse),term);
		}
	
//		//CLASS DRAW
//		for(Entry<String,Double> draw : drawContents.entrySet()){
//			String term = draw.getKey();
//			termWinTrue = (double) draw.getValue();
//			termWinFalse = (double) totalDrawTweets - termWinTrue;
//			//System.out.println(term + " " + termWinTrue + " " + termWinFalse);
//			for(Entry<String,Double> win : winContents.entrySet()){
//				if(term.equals(win.getKey())){
//					termOtherTrue = (double) win.getValue();
//					break;
//				}else{
//					termOtherTrue += 0.0;
//				}
//			}
//			for(Entry<String,Double> lose : loseContents.entrySet()){
//				if(term.equals(lose.getKey())){
//					termOtherTrue += (double) lose.getValue();
//					break;
//				}else{
//					termOtherTrue += 0.0;
//      			}
//			}
//			termOtherFalse = totalTweets - termOtherTrue;
//			results.put(calcMutInf(termWinTrue,termWinFalse,termOtherTrue,termOtherFalse),term);
//		}
		
//		//CLASS LOSE
//		for(Entry<String,Double> lose : loseContents.entrySet()){
//			String term = lose.getKey();
//			termWinTrue = (double) lose.getValue();
//			termWinFalse = (double) totalLoseTweets - termWinTrue;
//			//System.out.println(term + " " + termWinTrue + " " + termWinFalse);
//			for(Entry<String,Double> win : winContents.entrySet()){
//				if(term.equals(win.getKey())){
//					termOtherTrue = (double) win.getValue();
//					break;
//				}else{
//					termOtherTrue += 0.0;
//				}
//			}
//			for(Entry<String,Double> draw : drawContents.entrySet()){
//				if(term.equals(draw.getKey())){
//					termOtherTrue += (double) draw.getValue();
//					break;
//				}else{
//					termOtherTrue += 0.0;
//				}
//			}
//			termOtherFalse = totalTweets - termOtherTrue;
//			results.put(calcMutInf(termWinTrue,termWinFalse,termOtherTrue,termOtherFalse),term);
//		}
		System.out.println("Finished!");
		System.out.println("Writing results...");
		//WRITE OUTPUT RESULT
		try{
			CsvWriter writer = new CsvWriter("mutinf_unigrams.csv");	

			for(Entry<Double,String> score : results.entrySet()){
				//System.out.println("Term :'"+ score.getValue() + "' and its MI score: "+score.getKey());
				String[] in = new String[2];
				in[0] = score.getValue();
				in[1] = String.valueOf(score.getKey());
				writer.writeRecord(in);
			}
			writer.flush();
			System.out.println("Finished!");
		}catch(Exception e){
			
		}
		
		
		
	}

	private static double calcMutInf(double N11,double N01,double N10, double N00) {
		
		//double N11=6413;double N10=4522;
		//double N01=89497;double N00=91388;
		
		double N1 = N11+N10;
		double N0 = N00+N01;
		
		double N = N0+N1;
		
		
		double r1 = (N11/(N)) * ( Math.log( (N*N11) /( (N11+N10)*(N11+N01) )) / Math.log(2) );
		double r2 = (N01/(N)) * ( Math.log( (N*N01) /( (N01+N00)*(N11+N01) )) / Math.log(2) );
		double r3 = (N10/(N)) * ( Math.log( (N*N10) /( (N11+N10)*(N10+N00) )) / Math.log(2) );
		double r4 = (N00/(N)) * ( Math.log( (N*N00) /( (N01+N00)*(N10+N00) )) / Math.log(2) );

		
		
		//System.out.println(r1+r2+r3+r4);
		return (r1+r2+r3+r4);
		
	}

	private static Map<String, Double> getTweets(String file) {
		
		Map<String,Double> contents = new HashMap<String,Double>();
		try{
			 CSVReader reader = new CSVReader(new FileReader(file));
			 
		     String[] line = null;
		   		     		
		     //Top Line in input file(from mapreduce) contains the totals.
		     line = reader.readNext();
		     if(file.contains("WIN")){
		    	 totalWinTweets = Double.parseDouble(line[0].split("\\s+")[1]);
		     }else if(file.contains("DRAW")){
		    	 totalDrawTweets = Double.parseDouble(line[0].split("\\s+")[1]);
		     }else if(file.contains("LOSE")){
		    	 totalLoseTweets = Double.parseDouble(line[0].split("\\s+")[1]);
		     }
			 			
			 String term = null;
			 double tf = 0;
				
			 //unigrams
			 while ((line = reader.readNext())!=null){
				term = line[0].split("\\s+")[0];
				tf =  Double.parseDouble(line[0].split("\\s+")[1]);
				contents.put(term, tf);
			 }
			 
			 String firstTerm = null;
			 String secondTerm = null;
			
			 //bigrams
			 //while ((line = reader.readNext())!=null){
					//firstTerm = line[0].split("\\s+")[0];
					//secondTerm = line[0].split("\\s+")[1];
					//tf =  Double.parseDouble(line[0].split("\\s+")[2]);
					//contents.put((firstTerm + " " + secondTerm),tf);
			// }
				
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return contents;
	}

}
