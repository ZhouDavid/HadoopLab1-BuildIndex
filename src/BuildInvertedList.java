/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;

public class BuildInvertedList {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{
        public String name=new String();
        public void setup(Context context){
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName =  fileSplit.getPath().getName();
            this.name = fileName;
            System.err.println(this.name);
        }

        private Text word = new Text();
        private Text docName = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),"\t\n\r\f ,.[]?'-!;():&");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                docName.set(this.name);
                context.write(word,docName);
            }
        }
    }



    public static class TextArrayWritable extends ArrayWritable{
        public TextArrayWritable(){super(Text.class);}
        public TextArrayWritable(ArrayList<Text> names){
            super(Text.class);
            Text[] texts = new Text[names.size()];
            for(int i = 0;i<names.size();i++){
                texts[i] = new Text(names.get(i));
            }
            set(texts);
        }
    }


    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private TextArrayWritable result;
        private HashSet<Text> set=new HashSet<>();

        public boolean appeared(ArrayList<Text> unique_value,Text value){
            for(int i =0;i<unique_value.size();i++){
                if(value.equals(unique_value.get(i))){
                    System.err.println  ("i="+i+" value="+value+" and unique_valuei = "+unique_value.get(i));
                    return true;
                }
            }
            unique_value.add(value);
            return false;
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Text> unique_value = new ArrayList<>();
            Text filenames = new Text();
            for (Text val : values) {
                Text tmp= new Text(val);  //此处神坑，val似乎一直指向一个位置
                if(!appeared(unique_value,tmp)){
                    Text temp = new Text(val.toString()+" ");
                    filenames.append(temp.getBytes(),0,temp.getLength());
                }
            }
            context.write(key,filenames);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: build <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
