package util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import au.com.bytecode.opencsv.CSVReader;



public class Csving {

	public static void main(String[] args){
		
		try {
			CSVReader reader = new CSVReader(new FileReader(args[0]));
			
			String[] line = null;
			
			String[] tweets = new String[reader.readAll().size()-1];			
			
			reader = new CSVReader(new FileReader(args[0]));
			// read out header
			line = reader.readNext();
			int id = 0;
			
			while ((line = reader.readNext())!=null){
				tweets[id] = line[3];
				id++;
			}
			
			System.out.println(tweets[20256]);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
