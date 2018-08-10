package com.dataparse.server.service.parser.type;

import com.dataparse.server.util.DateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;
import org.joda.time.*;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.text.ParseException;
import java.util.*;


@Data
@Entity
@EqualsAndHashCode(callSuper = true, exclude = {"noTime"})
@NoArgsConstructor
@AllArgsConstructor
@DiscriminatorValue("d")
public class DateTypeDescriptor extends TypeDescriptor {

    private String pattern;
    private boolean noTime;

    public DateTypeDescriptor(final String pattern) {
        this.pattern = pattern;
    }

    @Override
    public DataType getDataType() {
        return DataType.DATE;
    }

    @Override
    public Object parse(String s) {
        return parse(s, pattern);
    }

    public static Object parse(String s, String pattern){
        if(pattern == null) {
            try {
                return new Date(Long.parseLong(s) * 1000);
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        } else {
            try {
                FastDateFormat format = DateUtils.getFormatByPattern(pattern);
                if (format == null) {
                    format = FastDateFormat.getInstance(pattern, TimeZone.getTimeZone("UTC"));
                }
                return format.parse(s);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static boolean hasNoTime(Date d){
        return d.equals(new DateTime(d).hourOfDay().roundFloorCopy().toDate());
    }

    public static DateTypeDescriptor forValue(Date d){
        DateTypeDescriptor descriptor = new DateTypeDescriptor();
        descriptor.setNoTime(DateTypeDescriptor.hasNoTime(d));
        return descriptor;
    }

    public static DateTypeDescriptor forString(String s){
        FastDateFormat format = DateUtils.tryGetDateFormat(s);
        DateTypeDescriptor descriptor = new DateTypeDescriptor(format != null ? format.getPattern() : null);
        Date d = (Date) descriptor.parse(s);
        descriptor.setNoTime(DateTypeDescriptor.hasNoTime(d));
        return descriptor;
    }

}
