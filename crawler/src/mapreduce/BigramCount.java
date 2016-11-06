/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



/**
 * <p>
 * Simple word count demo. This Hadoop Tool counts words in flat text file, and
 * takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-mappers] number of mappers</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * @author Jimmy Lin
 * @author Marc Sloan
 */
public class BigramCount extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(BigramCount.class);

	/**
	 *  Mapper: emits (token, 1) for every pair occurrence
	 *
	 */
	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, WordPair, IntWritable> {

		/**
		 *  Store an IntWritable with a value of 1, which will be mapped 
		 *  to each word found in the test
		 */
		private final static IntWritable one = new IntWritable(1);
		/**
		 * reuse objects to save overhead of object creation
		 */
		private Text word = new Text();


		/**
		 * Mapping function. This takes the text input, converts it into a String which is split into 
		 * words, then each of the words is mapped to the OutputCollector with a count of 
		 * one. 
		 * 
		 * @param key Input key, not used in this example
		 * @param value A line of input Text taken from the data
		 * @param output Map from each word (Text) to its count (IntWritable)
		 */
		
		/*Whitespace characters*/
		private String pattern = ","; 
		/*New instance of WordPair to hold a two word sequence */
		private WordPair twoWordSeq = new WordPair();
		private WordPair indicator = new WordPair();
		
		public void map(LongWritable key, Text value, OutputCollector<WordPair, IntWritable> output,
				Reporter reporter) throws IOException {
			/*Get Tokens*/
			String[] tokens = value.toString().split(pattern);
			indicator.setWord("**");indicator.setNeighbor("");
			output.collect(indicator, one);
			for (int i = 0; i < tokens.length; i++) {
				/*Set Left Word*/
				 twoWordSeq.setWord(tokens[i]);
				/* This is to make sure our String array of the tokens does not fall out of bounds*/
				 int end = i+1;
				 if(!(end == tokens.length)){
					 /* Set right word */
					twoWordSeq.setNeighbor(tokens[end]);  
					if((twoWordSeq.getWord().toString().isEmpty() || twoWordSeq.getNeighbor().toString().isEmpty())){
                    	continue;
                    }
					/*emit the two word seq and int 1 */
					 output.collect(twoWordSeq, new IntWritable(1));
				 }
			}
		}
	}

	/**
	 * Reducer: sums up all the counts
	 *
	 */
	private static class MyReducer extends MapReduceBase implements
			Reducer<WordPair, IntWritable, WordPair, IntWritable> {

		/**
		 *  Stores the sum of counts for a word
		 */
		private final static IntWritable SumValue = new IntWritable();

		/**
		 *  @param key The Text word 
		 *  @param values An iterator over the values associated with this word
		 *  @param output Map from each word (Text) to its count (IntWritable)
		 *  @param reporter Used to report progress
		 */
		private IntWritable totalCount = new IntWritable();
		public void reduce(WordPair key, Iterator<IntWritable> values,
				OutputCollector<WordPair, IntWritable> output, Reporter reporter) throws IOException {
			
			totalCount.set(getTotalCount(values));
			output.collect(key, totalCount);

		}
	}
	/*To partition mappings of same keys to the same reducers, using the modulo operation*/
	private static class MyPartitioner extends MapReduceBase implements 
		Partitioner<WordPair, IntWritable> {

		@Override
		public int getPartition(WordPair key, IntWritable arg1, int numReduceTasks) {
			return Math.abs(key.getWord().hashCode()) % numReduceTasks; 			
		}
		
	}


	private static int getTotalCount(Iterator<IntWritable> values) {
		int count = 0;
		while(values.hasNext()){
			count += values.next().get();
		}
		return count;
	}
	/**
	 * Creates an instance of this tool.
	 */
	public BigramCount() {
	}

	/**
	 *  Prints argument options
	 * @return
	 */
	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		int mapTasks = 1;//Integer.parseInt(args[2]);
		int reduceTasks = 1;//Integer.parseInt(args[3]);

		sLogger.info("Tool: BigramCount");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of mappers: " + mapTasks);
		sLogger.info(" - number of reducers: " + reduceTasks);

		JobConf conf = new JobConf(BigramCount.class);
		conf.setJobName("BigramCount");

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		/**
		 *  Note that these must match the Class arguments given in the mapper 
		 */
		conf.setOutputKeyClass(WordPair.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setPartitionerClass(MyPartitioner.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(outputDir.toUri(), conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		JobClient.runJob(conf);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BigramCount(), args);
		System.exit(res);
	}
}
