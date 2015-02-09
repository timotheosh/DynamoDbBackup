package com.inin.purecloud.devops.util;
import com.amazonaws.auth.BasicSessionCredentials;
import org.ini4j.Ini;
import java.io.FileReader;

/**
 * Created by thawes on 2/9/15.
 *
 * Generates an AWS BasicSessionCredentials from users AWS_CONFIG_FILE.
 * This will never be needed on the running host, as that host will already
 * have the appropriate IAM rights.
 */
public class AuthToken {
    // This is the profile name to be used get the token from.
    private String profile;

    public AuthToken() {
        this.profile = new String("profile dev");
    }

    public AuthToken(String profile) {
        this.profile = profile;
    }

    public BasicSessionCredentials getToken() {
        String filePath;
        BasicSessionCredentials token = null;
        if (System.getenv("AWS_CONFIG_FILE") != null)
            filePath = System.getenv("AWS_CONFIG_FILE");
        else
            filePath=String.format("%s/.aws/config", System.getProperty("user.home"));
        try {
            FileReader fr = new FileReader(filePath);
            Ini ini = new Ini(fr);
            token = new BasicSessionCredentials(
                    ini.get(profile).fetch("aws_access_key_id"),
                    ini.get(profile).fetch("aws_secret_access_key"),
                    ini.get(profile).fetch("aws_session_token"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return token;
    }
}
