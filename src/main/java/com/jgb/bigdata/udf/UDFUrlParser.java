package com.jgb.bigdata.udf;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.commons.codec.binary.Base64;

import java.net.*;
import java.util.regex.*;

public class UDFUrlParser extends UDF {

    private static final Logger logger = Logger.getLogger(UDFUrlParser.class);

    public String evaluate(String url) {
        // default
        return evaluate (url, "title", false);
    }

    public String evaluate(String adop_url, String json_key, boolean isUrlDecode) {
        String adop_data = getDataParameterFromAdopUrl (adop_url) ;

        if(checkAdopDataWhetherEncodeUrl(adop_data))
            adop_data = getUrlDecode(adop_data) ;

        String decodeString = getBase64Decode (adop_data) ;

        String value = getJsonValueByKey(decodeString, json_key) ;

        if(isUrlDecode)
            value = getUrlDecode(value) ;

        return value;
    }

    private String getDataParameterFromAdopUrl(String adop_url) {
        String result = "";
        try {
            URL aUrl = new URL (adop_url);
            Pattern p = Pattern.compile("data=([^&]+)");
            Matcher m = p.matcher(aUrl.getQuery ()) ;
            if (m.find())
                result = m.group(1);
        } catch (Exception e) {
            logger.error (this.getClass().getName() + ", " + e.getMessage () + ", "+ e.getStackTrace ());
        } finally {
            return result;
        }
    }

    private boolean checkAdopDataWhetherEncodeUrl(String adop_data) {
        boolean isUrl = false;
        try {
            String check_adop_data = URLEncoder.encode (URLDecoder.decode (adop_data, "UTF-8"));
            if (check_adop_data.equals (adop_data)) {
                isUrl = true ;
            }
        } catch(Exception e) {
            logger.error (this.getClass().getName() + ", " + e.getMessage () + ", "+ e.getStackTrace ());
        } finally {
            return isUrl;
        }

    }

    private String getUrlDecode(String value) {
        String result = value ;
        try {
            result = URLDecoder.decode (value, "UTF-8");
        } catch (Exception e) {
            logger.error (this.getClass().getName() + ", " + e.getMessage () + ", "+ e.getStackTrace ());
        }
        finally {
            return result ;
        }
    }

    private String getJsonValueByKey(String json, String key) {
        String result = "" ;
        JSONParser jsonParser = new JSONParser () ;
        try {
            JSONObject object = (JSONObject) jsonParser.parse (json);
            result = (String)object.get(key) ;
        } catch( Exception e) {
            logger.error (this.getClass().getName() + ", " + e.getMessage () + ", "+ e.getStackTrace ());
        } finally {
            return result;
        }
    }

    private String getBase64Decode(String url) {
        String decodeString = "";
        try {
            decodeString = new String(Base64.decodeBase64 (url), "UTF-8") ;
        } catch (Exception e) {
            logger.error (this.getClass().getName() + ", " + e.getMessage () + ", "+ e.getStackTrace ());
        }
        finally {
            return decodeString;
        }
    }
}
