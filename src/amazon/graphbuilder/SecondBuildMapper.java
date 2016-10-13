package amazon.graphbuilder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondBuildMapper extends Mapper<LongWritable, Text, Text, Text> {
    /*
     * The input is in the shape of "productId|title|total\t{score|userId\t}"
     */
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String record = inputValue.toString();
        String[] pieces = record.split("\t");
        String fixed = pieces[0];
        for (int i = 1; i < pieces.length; i++) {
            String[] var = pieces[i].split("\\|");
            String userId = var[1];
            int score = Integer.parseInt(var[0].trim());
            outputKey.set(userId);
            outputValue.set(fixed+"|"+score);
            context.write(outputKey, outputValue);
        }

    }

}