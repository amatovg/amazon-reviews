package amazon.graphbuilder;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondBuildReducer extends Reducer<Text, Text, Text, Text> {
    /*
     * The input is in the shape of Key:"userId"
     * Value:"{productId|title|total|score}"
     */
    private Text outputValue = new Text();
    private Text outputKey = new Text();

    public void reduce(Text inputKey, Iterable<Text> inputValues,
            Context context) throws IOException, InterruptedException {

        ArrayList<Record> list = new ArrayList<Record>();

        for (Text val : inputValues) {
            Record r = new Record(val.toString());
            if(!list.contains(r))
                list.add(new Record(val.toString()));
        }

        for (int i = 0; i < list.size(); i++) {

            Record one = list.get(i);

            this.outputKey.set(one.productId + "|" + one.title + "|" + one.total);
            for (int j = 0; j < list.size(); j++) {
                if (i != j) {
                    int oneStrength = 0;
                    int twoStrength = 0;
                    Record two = list.get(j);
                        if (one.score - two.score >= 2) {
                            oneStrength++;
                        } else if (two.score - one.score >= 2)
                            twoStrength++;
                        this.outputValue.set(two.productId + "|" + oneStrength
                                + "|" + twoStrength);
                        context.write(outputKey, outputValue);
                                        }
            }
        }

    }

    /*
     * The output is of the form
     * "productId|title|total\tproductId|oneStrength|twoStrength"
     */
    class Record {
        String productId;
        String title;
        int total;
        int score;

        public Record(String text) {
            String[] parts = text.split("\\|");
            productId = parts[0].trim();
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
            score = Integer.parseInt(parts[parts.length-1]);
        }

        @Override
        public boolean equals(Object v) {
            boolean retVal = false;
            if(v instanceof Record){
                Record ptr = (Record) v;
                String title1 = this.title.replaceAll("\\W", "").toLowerCase();
                String title2 = ptr.title.replaceAll("\\W", "").toLowerCase();
                int len = (title1.length()>title2.length()) ? title2.length() : title1.length();
                title1 = title1.substring(0, len/2);title2=title2.substring(0,len/2);
                retVal = ptr.productId.equals(this.productId) || title1.equals(title2);
            }
            return retVal;
        }
    }

}
