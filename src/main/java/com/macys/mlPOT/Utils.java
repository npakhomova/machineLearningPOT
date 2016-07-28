package com.macys.mlPOT;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by npakhomova on 6/9/16.
 */
public class Utils {
    public static final Gson gson = new Gson();



    static JavaSparkContext createSqlContext() {
        SparkConf config = new SparkConf();
        config.setMaster("local[16]");
        config.setAppName(WearToWorkPrepareTrainingSetJob.class.getName());

        return new JavaSparkContext(config);
    }

    static Map<String, String> prepareMcomOptions(JavaSparkContext sparkContext) {
        Map<String, String> options = new HashMap<String, String>();

        int partitions = sparkContext.defaultParallelism();
        options.put("driver", "oracle.jdbc.OracleDriver");
        options.put("user", "macys");
        options.put("password", "macys");
        options.put("url", "jdbc:oracle:thin:@//dml1-scan.federated.fds:1521/dpmstg01"); //mcom

        options.put("partitionColumn", "ID_MOD");
        options.put("lowerBound", "1");
        options.put("upperBound", String.valueOf(partitions));
        options.put("numPartitions", String.valueOf(partitions));
        return options;
    }

    static Map<String, String> prepareBcomOptions(JavaSparkContext sparkContext) {
        Map<String, String> options = new HashMap<String, String>();

        int partitions = sparkContext.defaultParallelism();
        options.put("driver", "oracle.jdbc.OracleDriver");
        options.put("user", "macys");
        options.put("password", "macys");
        options.put("url", "jdbc:oracle:thin:@//dml1-scan.federated.fds:1521/bpmstg01"); //bcom

        options.put("partitionColumn", "ID_MOD");
        options.put("lowerBound", "1");
        options.put("upperBound", String.valueOf(partitions));
        options.put("numPartitions", String.valueOf(partitions));
        return options;
    }



    public static void reCreateFolder(String path, boolean deleteExisting) throws IOException {
        File file = new File(path);
        if (file.exists() && deleteExisting) {
            System.out.println("ALARM!!! remove working folder: " + path);
            FileUtils.deleteDirectory(file);
        } else if (!file.exists()) {
            file.mkdirs();
        }

    }

    public static String buildURL(int imageId, String suffix, String prefix) {
        StringBuilder builder = new StringBuilder("/");
        for (int i = 1; i < 9; i++) {

            builder.append(imageId % (int) Math.pow(10, i) / (int) Math.pow(10, i - 1));
            if ((i % 2) == 0) {
                builder.append("/");
            }
        }
        return prefix + String.format("%sfinal/%d-" + suffix, builder.reverse().toString(), imageId);
    }

    public static File downOrloadImage(String urlString, String path) {
        try {
            String[] splittedUrl = urlString.split("/");
            String fileName = splittedUrl[splittedUrl.length - 1];
            final URL url = new URL(urlString);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            String mimeType = connection.getContentType();
            final BufferedImage img = ImageIO.read(connection.getInputStream());
            if (img != null) {
                return getTempFileByName(path, fileName, img, "jpg");
            } else {
                System.out.println("Incorrect URL of image. Mime type:" + mimeType);
            }
        } catch (IOException ex) {
            System.out.println(ex);
        }
        return null;
    }

    private static File getTempFileByName(String path, String fileName, BufferedImage img, String imageFormat) throws IOException {
        ImageIO.write(img, imageFormat, new File(path, fileName));
        return new File(path, fileName).isFile() ? new File(path, fileName) : null;
    }

    public static void writeToJson(String fileName, Object object) throws IOException {
        FileWriter writer = new FileWriter(fileName);
        writer.write(gson.toJson(object));
        writer.close();
    }

}
