package amazon.graphcompeting;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopCompetingReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();
    private Text outputKey = new Text();
    private final DecimalFormat df = new DecimalFormat("#0.000");
    private final String url = "http://www.amazon.com/dp/";

    public void reduce(Text inputKey, Iterable<Text> inputValues,
            Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<String>();
        for (Text text : inputValues) {
            list.add(text.toString());
        }
        if (list.size() <= 1)
            return;
        String title2 = "";
        for (String el : list) {
            if (!el.startsWith("GRAPH\t")) {
                title2 = el;
                break;
            }
        }
        for (String line : list) {
            if (line.startsWith("GRAPH\t")) {
                String[] parts = line.split("\t");
                String PID1 = parts[1];
                // String total = parts[2];
                String PID2 = parts[3];
                if(PID1.compareTo(PID2)>0){
                    int strength = Integer.parseInt(parts[4]);
                    int str1 = Integer.parseInt(parts[5])+1;
                    int str2 = Integer.parseInt(parts[6])+1;
                    String title1 = parts[7];
                    for (int i = 8; i < parts.length; i++) {
                        title1 += parts[i];
                    }
                    double metric1 = (str1) * (str2)/ (double) strength;
                    int maxStr = (str1>str2)?str1:str2;
                    int minStr = (str1>str2)?str2:str1;
                    double metric2 = maxStr/(double)strength;
                    double metric3 = maxStr*maxStr*minStr / (double) strength;
                    outputKey.set(df.format(metric1)+"\t"+df.format(metric2)+"\t"+df.format(metric3));
                    outputValue.set( strength + "\t"
                            + (str1-1) + "\t" + (str2-1) + "\t" + title1 + "\t" + title2 + "\t" + url + PID1 + "\t" + url
                            + PID2);
                    context.write(outputKey, outputValue);
                }
            }

        }

    }

}
