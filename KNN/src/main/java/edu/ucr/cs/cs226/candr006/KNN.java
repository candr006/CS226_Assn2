package edu.ucr.cs.cs226.candr006;
import java.io.*;
import java.nio.file.Paths;
import java.util.Random;
import java.util.StringTokenizer;
import net.minidev.json.JSONObject;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.commons.compress.compressors.CompressorInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.codehaus.jettison.json.JSONException;
import java.io.IOException;
import static java.nio.file.Files.probeContentType;
import static java.lang.Math.*;

/**
 * KNN
 *
 */

public class KNN
{
    public static class KNNMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String[] words =word.toString().split(",");
                Text key2= new Text(words[1]+','+words[2]);


                //calculate the distance between this point and q
                Configuration conf = context.getConfiguration();
                String q_string= conf.get("q");
                String[] q=q_string.split(",");
                double x1= Double.parseDouble(q[0]);
                double y1= Double.parseDouble(q[1]);

                double x2=Double.parseDouble(words[1]);
                double y2=Double.parseDouble(words[2]);

                double d=Math.sqrt(Math.pow((x2-x1),2)+Math.pow((y2-y1),2));
                final DoubleWritable dist = new DoubleWritable(d);

                context.write(key2, dist);
            }
        }
    }

    public static class KNNReducer
            extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public void MapReduceKNN(){

        return;
    }


    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        //check that all arguments are there
        if(args.length<3){
            System.out.println("\n\nERROR: You are missing one or more arguments.");
            System.out.println("<local file path> <point q>");
            System.out.println("Exiting");
            return;
        }
        String str_local_file=args[1];

        //check if the local file exists
        File localFile= new File(str_local_file);
        if(!localFile.exists()){
            System.out.println("\n\nERROR: The local file you entered does not exist. Exiting.\n");
            return;
        }

        //first decompress bzip file
        FileInputStream is4 = new FileInputStream(localFile);
        BZip2CompressorInputStream inputStream4 = new BZip2CompressorInputStream(is4, true);
        OutputStream ostream4 = new FileOutputStream("local_copy.csv");
        final byte[] buffer4 = new byte[8192];
        int n4 = 0;
        while ((n4 = inputStream4.read(buffer4))>0) {
            ostream4.write(buffer4, 0, n4);
        }
        ostream4.close();
        inputStream4.close();

        Configuration conf = new Configuration();
        conf.set("q", args[2]);
        Job job = Job.getInstance(conf, "knn");
        job.setJarByClass(KNN.class);
        job.setMapperClass(KNNMapper.class);
        job.setCombinerClass(KNNReducer.class);
        job.setReducerClass(KNNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("local_copy.csv"));
        FileOutputFormat.setOutputPath(job, new Path("KNN_output.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
