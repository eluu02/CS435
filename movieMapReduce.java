package compiled; //change this accordingly to your own file structure
    
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.conf.Configured;

public class movieMapReduce extends Configured implements Tool {

    public static Map<String, Double> titleMovieIDs = new HashMap<>();

    public static class movieMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts.length >= 6 && !parts[1].equalsIgnoreCase("movieId")) {
                String title = parts[0];
                double movieID = Double.parseDouble(parts[1]);
                double rating = Double.parseDouble(parts[3]);
                String genres = parts[5];
                titleMovieIDs.put(title, movieID);

                Pattern pattern = Pattern.compile("'name':\\s*'([^']+)'");
                Matcher matcher = pattern.matcher(genres);
                while (matcher.find()) {
                    String genre = matcher.group(1);
                    context.write(new Text(genre), new Text(movieID + "\t" + rating));
                }
            }
        }
    }

    public static class movieReducer extends Reducer<Text, Text, Text, Text> {
        private static final int K = 10;  // Top K movies per genre
    
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Double, Ratings> mapOutput = new HashMap<>();
            
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length == 2) {
                    try {
                        double movieID = Double.parseDouble(parts[0]);
                        double rating = Double.parseDouble(parts[1]);
                        Ratings movieRatings = mapOutput.get(movieID);
                        if (movieRatings == null) {
                            movieRatings = new Ratings(movieID, rating);
                            mapOutput.put(movieID, movieRatings);
                        }
                        else {
                            movieRatings.aggregateRating(rating);
                        }
                    } catch (NumberFormatException e) {
                        //xyz error
                        continue;
                    }
                }
            }
            List<Ratings> movieList = new ArrayList<>(mapOutput.values());  
            Collections.sort(movieList, (first, second) -> Double.compare(second.getAverage(), first.getAverage()));
            StringBuilder output = new StringBuilder();
            int count = 0;
            for (Ratings currMovie : movieList) {
                if (count < K) {
                    if (count > 0) {
                        output.append(" | ");
                    }
                    output.append("MovieID: ").append(currMovie.getID())
                          .append(", Average Rating: ").append(String.format("%.2f", currMovie.getAverage()));
                    count++;
                }
            }
    
            context.write(key, new Text(output.toString()));
        }
    }
    

    public static class Ratings {
        private double movieID;
        private int movieCount;
        private double ratingSum;

        //constructor
        public Ratings (double movieID, double rating) {
            this.movieID = movieID;
            this.movieCount = 1;
            this.ratingSum = rating;

        }
        //getters

        public double getID(){ 
            return this.movieID;
        }

        public double getAverage() {
            return this.movieCount == 0 ? 0 : this.ratingSum / this.movieCount;
        }


        public void aggregateRating(double rating) {
            this.ratingSum += rating;
            this.movieCount++;
        }
    }//end Ratings class

public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
        // function to run job

        Job job = Job.getInstance(conf, "movie");

        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(movieMapReduce.class);

        job.setMapperClass(movieMapReduce.movieMapper.class);
        job.setReducerClass(movieMapReduce.movieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new movieMapReduce(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        if (runJob(conf, args[0], args[1]) !=0) {
            return 1;
        } else {
            return 0;
        }
    }
}