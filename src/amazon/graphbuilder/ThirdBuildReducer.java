package amazon.graphbuilder;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdBuildReducer extends Reducer<Text, Text, Text, Text> {
    /*
     * The input is in the shape of Key: "pID1|title|total|pID2"
     * Value:"{oneStrength|twoStrength}"
     */
    private Text outputValue = new Text();
    private Text outputKey = new Text();

    public void reduce(Text inputKey, Iterable<Text> inputValues,
            Context context) throws IOException, InterruptedException {
        String pID1;
        String title;
        String pID2;
        int total;
        String[] parts = inputKey.toString().split("\\|");
        pID1 = parts[0];
        if(parts.length>4){
            title="";
            for (int i = 1; i < parts.length-3; i++) {
                title+=parts[i];
                title+="|";
            }
            title+=parts[parts.length-3];
        } else{
            title = parts[1];
        }
        total = Integer.parseInt(parts[parts.length-2]);
        pID2 = parts[parts.length-1];

        int strength = 0, oneStrength = 0, twoStrength = 0;
        for (Text val : inputValues) {
            strength++;
            String[] strengths = val.toString().split("\\|");
            oneStrength += Integer.parseInt(strengths[0]);
            twoStrength += Integer.parseInt(strengths[1]);
        }
        if (strength > context.getConfiguration().getInt("minStrength", 0)
                && strength < total*0.9 && strength*0.02 < (oneStrength+twoStrength)) {
            outputKey.set("GRAPH");
            outputValue.set(pID1+"\t"+total+ "\t" + pID2 +"\t"+strength + "\t" + oneStrength + "\t" + twoStrength+"\t"+title);
            context.write(outputKey, outputValue);
        }

    }

    /*
     * The output is in the shape of
     * "pID1|total|pID2\t strength|oneStrength|twoStrength"
     */

}
