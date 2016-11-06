package analysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class Init {

	public static void main(String[] args){
		Twokenize t = new Twokenize();
		
		String[] arguments = null;
		ArrayList<String> ar = new ArrayList<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(args[0]));
			
			String currentLine;
			
			while((currentLine = br.readLine())!= null){
				//System.out.println(currentLine);
				ar.add(currentLine);
				
			}
		
			arguments = new String[ar.size()];
			for(int i=0;i<ar.size();i++){
				arguments[i]=ar.get(i);
			}
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	
}
