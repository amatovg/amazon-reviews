package amazon.graphbuilder;

import org.apache.hadoop.util.ToolRunner;

public class Run {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("ERROR1: Usage: CMD <input path> <output path> <minimum reviews> <minimum strength>");
            for (String string : args) {
                System.out.println(string);
            }
            System.exit(0);
        }

        //String inputFolder = args[args.length-4];
        String outputFolder = args[args.length-3];
        //String minReviews = args[args.length-2];
        //String minStrength = args[args.length-1];

        String intermediateFolder1 = outputFolder + "-tmp1"; // output folder for the first job
        String intermediateFolder2 = outputFolder + "-tmp2"; // output folder for the second job

        args[args.length-3] = intermediateFolder1;
        int res = ToolRunner.run(new FirstBuild(), args);

        args[args.length-4] = intermediateFolder1;
        args[args.length-3] = intermediateFolder2;
        res += ToolRunner.run(new SecondBuild(), args);

        args[args.length-4] = intermediateFolder2;
        args[args.length-3] = outputFolder;
        res += ToolRunner.run(new ThirdBuild(), args);

        System.exit(res);
    }
}
