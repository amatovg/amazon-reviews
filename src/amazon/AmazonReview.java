package amazon;

import java.util.Calendar;
import com.google.gson.Gson;

public class AmazonReview {

    public String productId;
    public String title;
    public double price;
    public String userId;
    public String profileName;
    public int[] helpfulness;
    public double score;
    public int time;
    public String summary;
    public String text;


    /** No-args constructor required by Gson. */
    public AmazonReview() {}


    public static AmazonReview fromJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, AmazonReview.class);
    }


    public Calendar getCalendar() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time * 1000L);
        return cal;
    }


    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("product/productId: %s\n", this.productId));
        builder.append(String.format("product/title: %s\n", this.title));
        if (this.price >= 0) {
            builder.append(String.format("product/price: %.2f\n", this.price));
        } else {
            builder.append("product/price: unknown\n");
        }
        builder.append(String.format("review/userId: %s\n", this.userId));
        builder.append(String.format("review/profileName: %s\n", this.profileName));
        builder.append(String.format("review/helpfulness: %d/%d\n", this.helpfulness[0], this.helpfulness[1]));
        builder.append(String.format("review/score: %.1f\n", this.score));
        builder.append(String.format("review/time: %d\n", this.time));
        builder.append(String.format("review/summary: %s\n", this.summary));
        builder.append(String.format("review/text: %s\n", this.text));
        return builder.toString();
    }

}
