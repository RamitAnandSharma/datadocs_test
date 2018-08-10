package com.dataparse.server.util;

public class MathUtils {

    /**
     * Basically round to the next-to-last digit on the left. As an example:
     * Estimate: 182,999
     * Rounded: ~ 180,000
     * (since 8 is the second-to-left digit)
     *
     * Yep, I know..
     */
    public static long roundToNextToLastDigitOnTheLeft(long l){
        String s = Long.toString(l);
        if(s.length() < 3){
            return l;
        }
        char c1 = s.charAt(1);
        char c2 = s.charAt(2);
        boolean ceil = Byte.parseByte(Character.toString(c2)) >= 5;
        String result = Character.toString(s.charAt(0));
        result += Byte.parseByte(Character.toString(c1)) + (ceil ? 1 : 0);
        for(int i = 0; i < s.length() - 2; i++){
            result += "0";
        }
        return Long.parseLong(result);
    }
}
