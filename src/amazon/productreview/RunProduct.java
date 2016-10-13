package amazon.productreview;

import org.apache.hadoop.util.ToolRunner;

import amazon.userreview.Cumulate;


public class RunProduct {

    public static void main(String[] args) throws Exception {
        String outputFolder = args[args.length-1];
        String intermediateFolder = outputFolder + "-tmp"; // output folder for the first job


        args[args.length-1] = intermediateFolder;
        int res = ToolRunner.run(new ProductReviews(), args);
        args[args.length-2] = intermediateFolder;
        args[args.length-1] = outputFolder;
        res += ToolRunner.run(new Cumulate(), args);
        System.exit(res);
    }

}
