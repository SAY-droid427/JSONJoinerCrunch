import java.util.*;
import java.io.IOException;
import org.apache.crunch.fn.Aggregators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.Aggregator;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductCombiner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ProductCombiner(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf= getConf();
        conf.set("mapred.job.queue.name","d_bi");

        String inputPath1 = args[0];
        String inputPath2 = args[1];
        String inputPath3 = args[2];
        String outputPath = args[3];

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(ProductCombiner.class, getConf());

        // Reference a given text file as a collection of Strings.
        PCollection<String> lines1 = pipeline.readTextFile(inputPath1);
        PCollection<String> lines2 = pipeline.readTextFile(inputPath2);
        PCollection<String> lines3 = pipeline.readTextFile(inputPath3);
        PCollection<String> lines = lines1.union(lines2, lines3);

        PTable<String, String> id_pairs = lines.parallelDo(new GetWords(), Writables.tableOf(Writables.strings(),Writables.strings()));
        PTable<String, Collection<String>> coll=id_pairs.collectValues();

        PCollection<String> joinedFile = coll.parallelDo(new JoinFiles(), Writables.strings());

        // Instruct the pipeline to write the resulting counts to a text file.
        pipeline.writeTextFile(joinedFile, outputPath);

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}