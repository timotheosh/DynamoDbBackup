package com.inin.purecloud.devops.util;

/**
 * Created by thawes on 2/4/15.
 */
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Scanner;

public class Conversions {
    /**
     * Class providing some useful conversions for working with AWS Java SDK.
     * @param is
     * @return
     */
    public String convertStreamToString(InputStream is) {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    public InputStream convertStringToStream(String str) {
        InputStream is = new ByteArrayInputStream(str.getBytes());
        return is;
    }
}
