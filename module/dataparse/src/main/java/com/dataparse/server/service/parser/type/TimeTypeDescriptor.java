package com.dataparse.server.service.parser.type;


import com.dataparse.server.util.DateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.text.ParseException;
import java.util.*;

@Data
@Entity
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@DiscriminatorValue("t")
public class TimeTypeDescriptor extends TypeDescriptor {

    private String pattern;

    @Override
    public DataType getDataType() {
        return DataType.TIME;
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

    public static TimeTypeDescriptor forString(String s){
        FastDateFormat format = DateUtils.tryGetTimeFormat(s);
        return new TimeTypeDescriptor(format != null ? format.getPattern() : null);
    }
}