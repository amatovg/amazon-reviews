package amazon.scoretime;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreTimeReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text outputValue = new Text();

    public void reduce(Text inputKey, Iterable<DoubleWritable> inputValues, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        int size=0;
        ArrayList<Double> list = new ArrayList<Double>();
        for (DoubleWritable input: inputValues) {
            double val = input.get();
            sum += val;
            size++;
            list.add(val);
        }
        double avg = sum / size;
        sum=0;
        for(Double value:list) {
            sum+= Math.pow(value - avg,2);
        }
        double stddev = sum / size;
        stddev = Math.sqrt(stddev);

        DecimalFormat df = new DecimalFormat("0.000");
        this.outputValue.set(df.format(avg)+"\t"+df.format(stddev)+"\t"+size);
        context.write(inputKey, outputValue);
    }

}
