package com.jgb.bigdata;

import com.jgb.bigdata.udf.*;

public class Main {
    public static void main(String[] args) {
        UDFUrlParser udf = new UDFUrlParser ();
        System.out.println(udf.evaluate ("aa")) ;
    }
}
