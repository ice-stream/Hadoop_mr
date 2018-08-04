package com.ice.bigdata.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Alan
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable >  {

	
   /*	  对闯入的数据进行mapper分类*/
	
/*	String line = value.toString();
	//根据空格将这一行切分成单词
	String[] words = line.split(" ");
	
	//将单词输出为<单词，1>
	for(String word:words){
		//将单词作为key，将次数1作为value，以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reduce task
		context.write(new Text(word), new IntWritable(1));*/
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

           String line=value.toString();
           String[] words=line.split(" ");
           
           for(String word:words) {
             
        	     context.write(new Text(word), new IntWritable(1));
        	   
           }
		
	}

	
	
	    
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
