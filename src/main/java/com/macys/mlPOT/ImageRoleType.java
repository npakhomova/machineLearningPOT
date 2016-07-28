package com.macys.mlPOT;

/**
 * Created by npakhomova on 6/9/16.
 */
public enum ImageRoleType {
    CPRI("214x261.jpg"),
    CSW("214x214.jpg"),
    CADD("214x261.jpg");

    private String suffix;

    ImageRoleType(String suffix) {

        this.suffix = suffix;
    }

    public String getSuffix() {
        return suffix;
    }
}
