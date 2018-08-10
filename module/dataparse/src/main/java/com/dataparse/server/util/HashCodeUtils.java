package com.dataparse.server.util;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.util.List;

public class HashCodeUtils {

    public static String getSH1Code(List<Object> obj) {
        StringBuffer sb = null;
        try {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o = null;
            o = new ObjectOutputStream(b);
            o.writeObject(obj);

            MessageDigest mDigest = MessageDigest.getInstance("SHA1");
            byte[] result = mDigest.digest(b.toByteArray());
            sb = new StringBuffer();
            for (int i = 0; i < result.length; i++) {
                sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

}
