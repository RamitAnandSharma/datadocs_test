package com.dataparse.server.service.parser.processor;

import com.dataparse.server.service.parser.type.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.util.DateUtils;
import com.dataparse.server.util.FunctionUtils;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class FormatProcessor implements Processor {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");

    private Map<String, Agg> aggs;
    private Map<Show, Col> showColumns;
    private Map<Agg, Col> aggColumns;

    public FormatProcessor(BookmarkState state) {
        List<Agg> allAggs = new ArrayList<>();
        allAggs.addAll(state.getQueryParams().getAggs());
        allAggs.addAll(state.getQueryParams().getPivot());
        this.aggs = allAggs.stream().collect(Collectors.toMap(Agg::key, a -> a));
        this.showColumns = state
                .getQueryParams()
                .getShows()
                .stream()
                .collect(Collectors.toMap(s -> s, s -> state.getColumnList()
                        .stream()
                        .filter(c -> c.getField().equals(s.getField()))
                        .findFirst()
                        .get()));
        this.aggColumns = allAggs
                .stream()
                .collect(Collectors.toMap(a -> a, s -> state.getColumnList()
                        .stream()
                        .filter(c -> c.getField().equals(s.getField()))
                        .findFirst()
                        .get()));
    }

    private static int getQuarter(Date d) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        return (calendar.get(Calendar.MONTH) / 3) + 1;
    }

    private Object getFormattedLocationString(Object o) {
        if (o instanceof String) {
            LatLong latLong = GeoHash.decodeHash((String) o);
            return latLong.getLat() + ", " + latLong.getLon();
        } else if (o instanceof List) {
            return ((List) o).get(0) + ", " + ((List) o).get(1);
        }
        return o;
    }

    @Override
    public Object format(Object o, Col col, AggOp op) {
        return formatValue(o, col, op);
    }

    public static Object formatValue(Object o, Col col, AggOp op) {
        if (o == null) {
            return null;
        } else {
            ColFormat format = col.getSettings().getFormat();
            switch (col.getType()) {
                case STRING:
                    return o;
                case DATE:
                case TIME:
                    String pattern;
                    Long dateL = null;
                    if(o instanceof Double){
                        dateL = ((Double) o).longValue();
                    } else if (o instanceof Long) {
                        dateL = (Long) o;
                    }
                    Date date = new Date(dateL);

                    if (op == null) {
                        switch (format.getType()) {
                            case TEXT:
                                if (col.getType().equals(DataType.DATE)) {
                                    pattern = "EEE MMM d HH:mm:ss yyyy";
                                } else {
                                    pattern = "HH:mm:ss";
                                }
                                break;
                            case DATE_1:
                                pattern = "MM/dd/yyyy";
                                break;
                            case DATE_2:
                                pattern = "MMM. dd, yyyy";
                                break;
                            case TIME:
                                pattern = "H:mm:ss a";
                                break;
                            case DATE_TIME:
                                pattern = "MM/dd/yyyy HH:mm:ss";
                                break;
                            case DURATION:
                                pattern = "HH:mm:ss";
                                break;
                            default:
                                throw new RuntimeException(
                                        "Unknown date format type: " + format.getType());
                        }
                    } else {
                        switch (op) {
                            case YEAR:
                                pattern = "yyyy";
                                break;
                            case QUARTER:
                                pattern = "yyyy";
                                return "Q" + getQuarter(date) + ", " + FastDateFormat
                                        .getInstance(pattern, TimeZone.getTimeZone("UTC"))
                                        .format(date);
                            case MONTH:
                                pattern = "MMMM, yyyy";
                                break;
                            case DAY:
                                pattern = "yyyy-MM-dd";
                                break;
                            case HOUR:
                                pattern = "yyyy-MM-dd HH:mm";
                                break;
                            default:
                                throw new RuntimeException("Unknown operation: " + op);
                        }
                    }
                    return FastDateFormat.getInstance(pattern, TimeZone.getTimeZone("UTC")).format(date);
                case DECIMAL:
                    DecimalFormat df = new DecimalFormat(
                            format.getDecimalPlaces() > 0 ? "0." + StringUtils.repeat('0', format.getDecimalPlaces()) : "#");
                    switch (format.getType()) {
                        case NUMBER:
                            return df.format(o);
                        case PERCENT:
                            return df.format(o instanceof Long ? (long) o * 100 : (double) o * 100) + "%";
                        case FINANCIAL:
                            Double v = Double.valueOf(o.toString());
                            String f = df.format(v);
                            if (v < 0) {
                                return "-" + format.getCurrency() + f;
                            }
                            return format.getCurrency() + f;
                    }
                    break;
//                case LOCATION_LAT_LON:
//                    return getFormattedLocationString(o);
            }
        }
        return o;
    }


    public static String simpleFormatValue(Object o, Col col, ColFormat format) {
        DecimalFormat df = new DecimalFormat(
                format.getDecimalPlaces() > 0 ? "0." + StringUtils.repeat('0', format.getDecimalPlaces()) : "#");
        switch (format.getType()) {
            case TEXT:
                return o.toString();
            case NUMBER:
                return df.format(o);
            case PERCENT:
                return df.format(o instanceof Long ? (long) o * 100 : (double) o * 100) + "%";
            case FINANCIAL:
                Double v = Double.valueOf(o.toString());
                String f = df.format(v);
                if (v < 0) {
                    return "-" + format.getCurrency() + f;
                }
                return format.getCurrency() + f;
            case DATE_1:
                return toDate("MM/dd/yyyy", o);
            case DATE_2:
                return toDate("MM/dd/yyyy", o);
            case TIME:
                return toDate("H:mm:ss a", o);
            case DATE_TIME:
                return toDate("MM/dd/yyyy HH:mm:ss", o);
            case DURATION:
                return toDate("HH:mm:ss", o);
            case BOOLEAN_1:
                return formatAsBoolean(o, col) ? "Yes" : "No";
            case BOOLEAN_2:
                return formatAsBoolean(o, col) ? "TRUE" : "FALSE";
        }
        return null;
    }

    private static Boolean formatAsBoolean(Object val, Col col) {
        if(val == null) {
            throw new RuntimeException("Failed to parse boolean. Value is null.");
        }
        switch (col.getType()) {
            case STRING:
                return stringToBoolean((String) val);
            case DECIMAL:
                return decimalToBoolean(val.toString());
            default:
                throw new RuntimeException("Failed to parse boolean");
        }
    }

    private static Boolean decimalToBoolean(String str) {
        boolean isTrue = new Long(1L).equals(Long.parseLong(str));
        if(isTrue) {
            return true;
        }
        boolean isFalse = new Long(0L).equals(Long.parseLong(str));
        if(isFalse) {
            return false;
        }
        return null;
    }

    private static Boolean stringToBoolean(String str) {
        switch (str.toLowerCase()) {
            case "yes":
            case "true":
                return true;
            case "no":
            case "false":
                return false;
            default:
                return null;
        }
    }

    private static String toDate(String pattern, Object val) {
        Date date = null;
        if(val instanceof String) {
            date = DateUtils.parseDateISO((String) val);
        }

        boolean isAcceptableValue = val instanceof Long || val instanceof Double || date != null;
        if(val != null && isAcceptableValue) {
            return FastDateFormat.getInstance(pattern, TimeZone.getTimeZone("UTC")).format(date == null ? val : date);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> process(Map<String, Object> o, Map<String, Show> shows) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : o.entrySet()) {
            Col col = null;
            AggOp op = null;
            Object formatted = e.getValue();
            Show show = shows.get(e.getKey());
            if(show == null){
                Agg agg = aggs.get(e.getKey());
                if(agg != null) {
                    op = agg.getOp();
                    col = aggColumns.get(agg);
                }
            } else {
                col = showColumns.get(show);
            }

            if(col != null && (show == null || show.getOp() == null || show.getOp().isPreservesType())) {
                formatted = format((Object) e.getValue(), (Col) col, (AggOp) op);
            }
            result.put(e.getKey(), formatted);
        }
        return result;
    }


    public static Object tryToConvertValue(String val) {
        Long l = FunctionUtils.invokeSilent(() -> Long.parseLong(val));
        Double d = FunctionUtils.invokeSilent(() -> Double.parseDouble(val));
        return FunctionUtils.coalesce(l, d, val);
    }
}
