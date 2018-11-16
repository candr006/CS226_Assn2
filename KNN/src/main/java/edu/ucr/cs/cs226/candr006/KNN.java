package edu.ucr.cs.cs226.candr006;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.TreeMap;

/**
 * KNN
 *
 */
public class KNN
{

    public static class KNNMapper
            extends Mapper<Object, Text, DoubleWritable,Text>{
        private Text word = new Text();
        private TreeMap<DoubleWritable, Text> KDistMap = new TreeMap<DoubleWritable, Text>() ;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String[] words =word.toString().split(",");
                Text val= new Text(words[1]+','+words[2]);


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

                context.write(dist,val);

            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (DoubleWritable i : KDistMap.keySet()) {
                context.write(i, KDistMap.get(i));
            }
        }


    }

    public static class KNNReducer
            extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
        private DoubleWritable result = new DoubleWritable();
        public Integer k_iter= 0;
        public static Integer k;
        private TreeMap<DoubleWritable, Text> KDistMap = new TreeMap<DoubleWritable, Text>() ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.valueOf(conf.get("k"));
            k_iter= 0;
        }

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (DoubleWritable i : values) {

                if (KDistMap.size() < k) {
                    KDistMap.put(i, KDistMap.get(i));

                }
            }
        }

        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            try {
                while (context.nextKey() && (k_iter<k)) {
                    k_iter++;
                    reduce(context.getCurrentKey(), context.getValues(), context);
                    Iterator<Text> iter = context.getValues().iterator();
                    if(iter instanceof ReduceContext.ValueIterator) {
                        ((ReduceContext.ValueIterator<Text>)iter).resetBackupStore();
                    }
                }
            } finally {
                cleanup(context);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (DoubleWritable i : KDistMap.keySet()) {
                context.write(i, KDistMap.get(i));
            }
        }

    }


    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        //check that all arguments are there
        if(args.length<4){
            System.out.println("\n\nERROR: You are missing one or more arguments.");
            System.out.println("<local file path> <point q> <k>");
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

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MMddyyyyHHmmss");
        LocalDateTime now = LocalDateTime.now();
        String formatted = dtf.format(now);
        String out_path="KNN_output_"+formatted+".txt";

        Configuration conf = new Configuration();
        conf.set("q", args[2]);
        conf.set("k",args[3]);
        Job job = Job.getInstance(conf, "knn");
        job.setNumReduceTasks(1);
        job.setJarByClass(KNN.class);
        job.setMapperClass(KNNMapper.class);
        job.setCombinerClass(KNNReducer.class);
        job.setReducerClass(KNNReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("local_copy.csv"));

        FileOutputFormat.setOutputPath(job, new Path(out_path));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
