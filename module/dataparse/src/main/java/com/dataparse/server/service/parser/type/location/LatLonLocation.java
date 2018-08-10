package com.dataparse.server.service.parser.type.location;

import com.dataparse.server.service.flow.ErrorValue;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
@AllArgsConstructor
public class LatLonLocation{

    static final Pattern RegexLatlon = Pattern.compile("(\\-?\\d+(\\.\\d+)?)\\s*[,|/]\\s*(\\-?\\d+(\\.\\d+)?)");

    private Double lat;
    private Double lon;

    public static Object parse(String s) {
        Matcher m = RegexLatlon.matcher(s.trim());
        if (m.matches()) {
            Double lat1 = Double.parseDouble(m.group(1));
            Double lon1 = Double.parseDouble(m.group(3));
            if ((lat1 <= 90 && lat1 >= -90) &&
                    (lon1 <= 180 && lon1 >= -180)) {
                return new LatLonLocation(lat1, lon1);
            }
        }
        return new ErrorValue();
    }
}
