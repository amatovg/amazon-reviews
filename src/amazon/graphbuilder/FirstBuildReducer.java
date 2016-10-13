package amazon.graphbuilder;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstBuildReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();
    private Text outputKey = new Text();

    public void reduce(Text inputKey, Iterable<Text> inputValues, Context context)
            throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        int total = 0;
        ArrayList<String> values = new ArrayList<String>();
        for(Text val : inputValues){
            if(!values.contains(val.toString()))
                values.add(val.toString());
        }

        for(String val : values){
            builder.append(val.toString()+"\t");
            total++;
        }

        if(total > context.getConfiguration().getInt("minReviews", 0)){
            this.outputValue.set(builder.toString());
            this.outputKey.set(inputKey.toString()+"|"+total);
            context.write(outputKey, outputValue);
        }
    }

}

