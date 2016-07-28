package com.macys.mlPOT;


import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * Created by npakhomova on 6/9/16.
 */
public class WearToWorkPrepareTrainingSetJob {

    public static final String ROOT_FOLDER = "wearToWorkJob/";
    public static String STARS_SERVICE_PREFIX = "http://raymcompreviewprod"; //mcom
    //    public static String STARS_SERVICE_PREFIX = "http://raybcompreviewprod"; //bcom

    private static String query = "select distinct\n" +
            "  PRODUCT.PRODUCT_ID,\n" +
            "  PRODUCT.PRODUCT_DESC,\n" +
            "  PRODUCT.VENDOR_CODE,\n" +
            "  PRODUCT_IMAGE.IMAGE_ID,\n" +
            "\n" +
            "  PRODUCT_TYPE.PRODUCT_TYPE_NAME,\n" +
            "  PRODUCT_TYPE.SHORT_DESCRIPTION,\n" +
            "  PRODUCT_TYPE.PRODUCT_TYPE_DESCRIPTION,\n" +
            "  mod(PRODUCT_IMAGE.PRODUCT_ID, %d) AS ID_MOD \n" +
            "\n" +
            "from PRODUCT\n" +
            " join PRODUCT_TYPE on PRODUCT.PRODUCT_TYPE_ID = PRODUCT_TYPE.PRODUCT_TYPE_ID\n" +
            "  join UPC on UPC.PRODUCT_ID = PRODUCT.PRODUCT_ID and PRODUCT.STATE_ID = 2\n" +
            "  join PRODUCT_COLORWAY on PRODUCT_COLORWAY.PRODUCT_COLORWAY_ID =UPC.PRODUCT_COLORWAY_ID\n" +
            "  join PRODUCT_COLORWAY_IMAGE on PRODUCT_COLORWAY_IMAGE.PRODUCT_COLORWAY_ID = PRODUCT_COLORWAY.PRODUCT_COLORWAY_ID\n" +
            "\n" +
            "  join PRODUCT_IMAGE on PRODUCT_IMAGE.PRODUCT_IMAGE_ID = PRODUCT_COLORWAY_IMAGE.PRODUCT_IMAGE_ID "+
            "where CATEGORY_ID = 39096";

   
    public static void main(String[] args) throws IOException {

        JavaSparkContext sparkContext = Utils.createSqlContext();
        SQLContext sqlContext = new SQLContext(sparkContext);

        Map<String, String> options = Utils.prepareMcomOptions(sparkContext);


        String formatedQuery = String.format(query, sparkContext.defaultParallelism());
        DataFrame selectPositiveDataFrame = sqlContext.read().format("jdbc").options(options).option("dbtable", "(" + formatedQuery + ")").load();
        // download pictures

        JavaRDD<ProductWithUrlAndColor> productWithUrlAndColorJavaRDD = selectPositiveDataFrame.toJavaRDD().map(new Function<Row, ProductWithUrlAndColor>() {
            @Override
            public ProductWithUrlAndColor call(Row row) throws Exception {
                Integer image_id = row.<BigDecimal>getAs("IMAGE_ID").intValue();
                ProductWithUrlAndColor result = new ProductWithUrlAndColor();
                result.productType = row.<String>getAs("PRODUCT_TYPE_NAME");
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
