package com.macys.mlPOT;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Created by npakhomova on 7/13/16.
 */
public class DressLengthPrepareDataJob {

    public static final String ROOT_FOLDER = "dressLengthOnlyPublished/";
    public static String STARS_SERVICE_PREFIX = "http://raymcompreviewprod"; //mcom
    //    public static String STARS_SERVICE_PREFIX = "http://raybcompreviewprod"; //bcom

    private static String query = "select distinct\n" +
            "  PRODUCT.PRODUCT_ID,\n" +
            "  PRODUCT_IMAGE.IMAGE_ID,\n" +
            "  PRODUCT_ATTRIBUTE.varchar_value,\n" +
            "  mod(PRODUCT_IMAGE.PRODUCT_ID, %d) AS ID_MOD \n" +
            "  \n" +
            "from PRODUCT_IMAGE\n" +
            "join PRODUCT_ATTRIBUTE on PRODUCT_ATTRIBUTE.PRODUCT_ID = PRODUCT_IMAGE.PRODUCT_ID and  PRODUCT_ATTRIBUTE.attribute_type_id = 708\n" +
            "join PRODUCT on PRODUCT_IMAGE.PRODUCT_ID = PRODUCT.PRODUCT_ID and PRODUCT.STATE_ID = 2\n"+
            "join PRODUCT_DESTINATION_CHANNEL ON PRODUCT_DESTINATION_CHANNEL.PRODUCT_ID = PRODUCT.PRODUCT_ID AND PRODUCT_DESTINATION_CHANNEL.PUBLISH_FLAG='Y' AND PRODUCT_DESTINATION_CHANNEL.CURRENT_FLAG='Y'\n";


    public static void main(String[] args) throws IOException {

        JavaSparkContext sparkContext = Utils.createSqlContext();
        SQLContext sqlContext = new SQLContext(sparkContext);

        Map<String, String> options = Utils.prepareMcomOptions(sparkContext);


        String formatedQuery = String.format(query, sparkContext.defaultParallelism());
        DataFrame selectPositiveDataFrame = sqlContext.read().format("jdbc").options(options).option("dbtable", "(" + formatedQuery + ")").load();
        // download pictures
        selectPositiveDataFrame.show(10);

        JavaRDD<ProductWithUrlAndColor> productWithUrlAndColorJavaRDD = selectPositiveDataFrame.toJavaRDD().map(new Function<Row, ProductWithUrlAndColor>() {
            @Override
            public ProductWithUrlAndColor call(Row row) throws Exception {
                Integer image_id = row.<BigDecimal>getAs("IMAGE_ID").intValue();
                ProductWithUrlAndColor result = new ProductWithUrlAndColor();
                result.productType = row.<String>getAs("VARCHAR_VALUE");
                final String path = ROOT_FOLDER + "/" + result.productType;
                Utils.reCreateFolder(path, false);

                String urlString = Utils.buildURL(image_id.intValue(), ImageRoleType.CPRI.getSuffix(), STARS_SERVICE_PREFIX);
                File picture = Utils.downOrloadImage(urlString, path);

                result.productId = row.<BigDecimal>getAs("PRODUCT_ID").intValue();
                if (picture != null) {
                    result.imagePath = picture.getAbsolutePath();
                }
                result.imageUrl = urlString;


                return result;
            }
        });
        productWithUrlAndColorJavaRDD.collect();

    }
}
