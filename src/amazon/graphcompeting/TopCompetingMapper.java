package amazon.graphcompeting;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TopCompetingMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {
        String line = inputValue.toString();


        if(line.startsWith("GRAPH")){
            String[] parts = line.split("\t");
            outputKey.set(parts[3]); //PID2
            outputValue.set(line);
        } else if(line.length()>11){
            outputKey.set(line.substring(0,10)); //PID
            outputValue.set(line.substring(11)); //title
        }

        context.write(outputKey, outputValue);


    }

}
