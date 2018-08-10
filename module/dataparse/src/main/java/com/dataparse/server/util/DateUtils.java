package com.dataparse.server.util;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DateUtils {

    public static final FastDateFormat bqDefaultFormat;
    public static final FastDateFormat isoFormat;
    public static final FastDateFormat timeFormat;
    public static final FastDateFormat shortDateFormat;

    public static final List<String> knownAmPmDateFormats;
    public static final List<FastDateFormat> knownDateFormats;
    public static final List<FastDateFormat> bigQuerySupportedFormats;
    public static final List<FastDateFormat> knownTimeFormats;

    public static final Map<String, FastDateFormat> knownFormatsMap;

    static {
        TimeZone UTC = TimeZone.getTimeZone("UTC");
        isoFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss'Z'", UTC);
        bqDefaultFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", UTC);
        timeFormat = FastDateFormat.getInstance("HH:mm:ss", UTC);
        shortDateFormat = FastDateFormat.getInstance("MMM dd, yyyy", UTC);

        bigQuerySupportedFormats =  Stream.of(
                "yyyy-MM-dd HH:mm",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss.ZZ"
                ).map(p -> FastDateFormat.getInstance(p, UTC)).collect(Collectors.toList());

        knownTimeFormats = Stream.of(
                "\'T\'HH:mm:ssZZ",
                "\'T\'HH:mm:ss",
                "HH:mm:ssZZ",
                "HH:mm:ss").map(p -> FastDateFormat.getInstance(p, UTC)).collect(Collectors.toList());

        knownAmPmDateFormats = Arrays.asList(
                "MM/d/yyyy hh:mm:ss a",
                "MM/dd/yyyy hh:mm:ss a",
                "M/d/yyyy hh:mm a",
                "M/d/yyyy hh:mm:ss a",
                "M/dd/yyyy hh:mm:ss a"
        );


        knownDateFormats = Stream.of(
                "E MMM dd hh:mm:ss z yyyy",
                "yyyy-MM-dd\'T\'HH:mm:ssZZ",
                "yyyy-MM-dd\'T\'HH:mm:ss",
                "yyyy-MM-dd\'T\'HH:mm:ss.S'Z'",
                "yyyy-MM-dd HH:mm:ss ZZ",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd ZZ",
                "yyyy-MM-ddZZ",
                "yyyy-MM-dd",

                // slash separator - USA
                "MM/dd/yy HH:mm:ss",
                "MM/dd/yyyy HH:mm:ss",
                "MM/dd/yy HH:mm",
                "MM/dd/yyyy HH:mm",
                "MM/dd/yy",
                "MM/dd/yyyy",
                "M/d/yy H:mm",
                "dd/mm/yyyy hh:mm:ss.SSSSS",

                // no separator, time zone included
                "EEE MMM dd HH:mm:ss z yyyy",

                // dot separator - Russia
                "dd.mm.yy HH:mm:ss",
                "dd.mm.yyyy HH:mm:ss",
                "dd.mm.yy HH:mm",
                "dd.mm.yyyy HH:mm",
                "dd.mm.yy",
                "dd.mm.yyyy",

                // hyphen separator - Netherlands
                "d-MMM-yy",
                "dd-MMM-yy",
                "dd-mm-yy HH:mm:ss",
                "dd-mm-yyyy HH:mm:ss",
                "dd-mm-yy HH:mm",
                "dd-mm-yyyy HH:mm",
                "dd-mm-yy",
                "dd-mm-yyyy").map(p -> FastDateFormat.getInstance(p, UTC)).collect(Collectors.toList());

        knownFormatsMap = new HashMap<>();
        knownFormatsMap.putAll(Maps.uniqueIndex(knownDateFormats, FastDateFormat::getPattern));
    }


    public synchronized static String formatDateISO(Date date) {
        if (date == null) {
            return null;
        }

        return isoFormat.format(date);
    }

    public synchronized static String formatToBQSupportedFormat(Date val) {
        return bqDefaultFormat.format(val);
    }

    public synchronized static Date parseDateISO(String date) {
        if (date == null) {
            return null;
        }
        try {
            return isoFormat.parse(date);
        } catch (ParseException e) {
            log.info("Can not parse date {}.", date, e);
        }
        return null;
    }

    public static FastDateFormat tryGetTimeFormat(String s){
        if(s.length() < 5 || (!Character.isDigit(s.charAt(0)) && s.charAt(0) != 'T')){
            return null;
        }

        for(FastDateFormat format : knownTimeFormats){
            try {
                ParsePosition position = new ParsePosition(0);
                Date date = format.parse(s);
                if(date != null && position.getErrorIndex() < 0){
                    return format;
                }
            } catch (Exception e) {
                continue;
            }
        }

        return null;
    }

    public static FastDateFormat tryGetDateFormat(String s){
        return tryGetDateFormat(s, knownDateFormats);
    }

    public static FastDateFormat tryGetDateFormat(String s, List<FastDateFormat> availableFormats){
        if(s == null || s.length() < 5) {
            return null;
        }

        // fast date format, common behaviour
        for(FastDateFormat format : availableFormats) {
            try {
                ParsePosition position = new ParsePosition(0);
                Date date = format.parse(s, position);
                if(date != null && position.getIndex() == s.length()){
                    return format;
                }
            } catch (Exception e) {
                continue;
            }
        }

        // simple date format for am\pm items (fastDateFormat does not parse last char 'm' in 'am'\'pm')
        for(String formatTemplate : knownAmPmDateFormats) {
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatTemplate, Locale.ENGLISH);
                ParsePosition position = new ParsePosition(0);
                Date date = simpleDateFormat.parse(s, position);

                if(date != null && position.getIndex() == s.length()) {
                    return FastDateFormat.getInstance(formatTemplate, TimeZone.getTimeZone("UTC"));
                }
            } catch (Exception ex) {
                continue;
            }
        }

        return null;
    }

    public static FastDateFormat getFormatByPattern(String pattern){
        return knownFormatsMap.get(pattern);
    }

}
