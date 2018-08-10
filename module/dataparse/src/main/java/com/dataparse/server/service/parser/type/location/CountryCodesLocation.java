package com.dataparse.server.service.parser.type.location;

import com.dataparse.server.service.flow.ErrorValue;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

@Data
@Slf4j
@AllArgsConstructor
public class CountryCodesLocation {

    private static Set<String> COUNTRY_CODES;
    private static final Pattern REGEX_CODES = Pattern.compile("^([a-zA-Z]){2,3}$");

    private String countryCode;

    static {
        try {
            COUNTRY_CODES = new HashSet<>(
                    Resources.readLines(
                            Resources.getResource("location/country_codes.txt"),
                            Charsets.UTF_8));
        } catch (IOException e) {
            log.error("Can't load country codes vocabulary", e);
        }
    }

    public static Object parse(String s) {
        if (REGEX_CODES.matcher(s).matches() && COUNTRY_CODES.contains(s.toLowerCase())) {
            return new CountryCodesLocation(s);
        }
        return new ErrorValue();
    }

    @Override
    public String toString() {
        return countryCode;
    }
}

