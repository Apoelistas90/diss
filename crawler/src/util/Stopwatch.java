package util;

public class Stopwatch {

	long initialTime;
	long elapsedTime;
	
	public Stopwatch()
	{
		this.initialTime = System.currentTimeMillis();
		this.elapsedTime = 0;
	}
	
	public void stop()
	{
		long currentTime = System.currentTimeMillis();
		this.elapsedTime = currentTime-this.initialTime;
		this.initialTime = currentTime;		
	}

	public void lap()
	{
		long currentTime = System.currentTimeMillis();
		this.elapsedTime = currentTime-this.initialTime;
	}
	
	public long getElapsedTime()
	{
		return this.elapsedTime;
	}
	
	public String toString()
	{
		return Stopwatch.toString(this.elapsedTime);		
	}
	
	public static String toString (long milliseconds)
	{
		long ms = milliseconds % 1000;
		
		milliseconds /= 1000;
		long seconds = milliseconds % 60;

		milliseconds /= 60;
		long minutes = milliseconds % 60;

		milliseconds /= 60;
		long hours = milliseconds;
		
		return ("Execution time: " + hours + "h - " + minutes + "min - " + seconds + "sec - " + ms + "ms");
		
	}

	
}