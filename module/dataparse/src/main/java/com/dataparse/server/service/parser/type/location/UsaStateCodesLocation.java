package com.dataparse.server.service.parser.type.location;

import com.dataparse.server.service.flow.ErrorValue;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

@Data
@Slf4j
@AllArgsConstructor
@EqualsAndHashCode
public class UsaStateCodesLocation implements Serializable {

    private static Set<String> USA_STATE_CODES;
    private static final Pattern REGEX_CODES = Pattern.compile("^([a-zA-Z]){2}$");

    private String usaStateCode;

    static {
        try {
            USA_STATE_CODES = new HashSet<>(
                    Resources.readLines(
                            Resources.getResource("location/usa_state_codes.txt"),
                            Charsets.UTF_8));
        } catch (IOException e) {
            log.error("Can't load USA state codes vocabulary");
        }
    }

    public static Object parse(String s) {
        if (REGEX_CODES.matcher(s).matches() && USA_STATE_CODES.contains(s.toLowerCase())) {
            return new UsaStateCodesLocation(s);
        }
        return new ErrorValue();
    }

    @Override
    public String toString() {
        return usaStateCode;
    }
}
