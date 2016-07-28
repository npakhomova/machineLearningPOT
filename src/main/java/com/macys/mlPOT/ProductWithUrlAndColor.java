package com.macys.mlPOT;

import java.io.Serializable;

/**
 * Created by npakhomova on 6/9/16.
 */
public class ProductWithUrlAndColor implements Serializable{
    Integer productId;
    String imageUrl;
    String imagePath;
    int colorNormalId;
    String productType;
    String dressLengthValue;
}
