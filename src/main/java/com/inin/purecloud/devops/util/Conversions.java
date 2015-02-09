package com.inin.purecloud.devops.util;

/**
 * Created by thawes on 2/4/15.
 *
 * This class provides a couple utility functions for Python to work with Java
 * more seamlessly, and also an easy class to load to reference files (ala
 * template files) within the jar file.
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
