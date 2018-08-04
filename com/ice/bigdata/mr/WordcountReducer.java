package com.ice.bigdata.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

 
/**
 * @author Alan
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
	       int count =0;
 
	       /*Iterator<IntWritable> iterator =values.iterator();
	       
	       while(iterator.hasNext()) {
	    	   count+=iterator.next().get();
	       }*/
	       for(IntWritable value:values) {
	    	   
	    	   
	    	   count +=value.get();
	       }
	       
	       context.write(key, new IntWritable(count));
		
		
		
	}

/*	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int count=0;
		Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			count += iterator.next().get();
		}
		
		for(IntWritable value:values){
		
			count += value.get();
		}
		
		context.write(key, new IntWritable(count));
		
	}*/
	
	
	
	
	  
	
	
	
	
}
