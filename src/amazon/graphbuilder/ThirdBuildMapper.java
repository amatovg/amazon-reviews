package amazon.graphbuilder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdBuildMapper extends Mapper<LongWritable, Text, Text, Text> {
    /*
     * The input is of the form
     * "productId|title|total\tproductId|oneStrength|twoStrength"
     */
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String record = inputValue.toString();
        String[] pieces = record.split("\t");
        String[] key = pieces[0].split("\\|");
        String[] value = pieces[1].split("\\|");
        String pID1 = key[0];
        String title;
        if(key.length>3){
            title="";
            for (int i = 1; i < key.length-2; i++) {
                title+=key[i];
                title+="|";
            }
            title+=key[key.length-2];
        } else{
            title = key[1];
        }

        String total = key[key.length-1];
        String pID2 = value[0];
        String oneStrength = value[1];
        String twoStrength = value[2];
        outputKey.set(pID1+"|"+title+"|"+total+"|"+pID2);
        outputValue.set(oneStrength+"|"+twoStrength);
        context.write(outputKey, outputValue);


    }
    /*
     * The output is in the shape of Key: "pID1|title1|total|pID2" Value:"{oneStrength|twoStrength}"
     */

}