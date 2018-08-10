package com.dataparse.server.service.upload;

import com.dataparse.server.service.upload.refresh.RefreshType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.TimeZone;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class RefreshSettings implements Serializable {

    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @NotNull
    RefreshType type;

    String cronExpression;

    // it's not timezone, it's the line in format 1000 or -0330, etc.. where 10 and -03 hour offset, and 00 and 30 minutes offset.
    String timeZone;

    boolean preserveHistory;

    public static RefreshSettings never() {
        return new RefreshSettings(null, RefreshType.NONE, null, null, false);
    }

    @Transient
    @JsonIgnore
    private Pair<Integer, Integer> getTimezoneHoursAndMinutes() {
        if (StringUtils.isEmpty(this.timeZone)) {
            return Pair.of(0, 0);
        }
        char[] chars = this.timeZone.toCharArray();
        int length = chars.length;
//        todo remove this after db cleanup
        if (length == 5 || length == 4) {
            String sign = length == 5 ? "-" : "";
            Integer minutesOffset = Integer.parseInt(sign + new String(Arrays.copyOfRange(chars, length - 2, length)));
            Integer hoursOffset = Integer.parseInt(sign + new String(Arrays.copyOfRange(chars, length - 4, length - 2)));
            return Pair.of(hoursOffset, minutesOffset);
        } else {
            return Pair.of(0, 0);
        }
    }

    @Transient
    @JsonIgnore
    public TimeZone defineTimeZone() {
        Pair<Integer, Integer> timezoneHoursAndMinutes = this.getTimezoneHoursAndMinutes();
        ZoneOffset offset = ZoneOffset.ofHoursMinutes(timezoneHoursAndMinutes.getKey(), timezoneHoursAndMinutes.getValue());
        return TimeZone.getTimeZone(ZoneId.ofOffset("GMT", offset));
    }

    @Override
    public String toString() {
        return "RefreshSettings{" +
                "type=" + type +
                ", cronExpression='" + cronExpression + '\'' +
                ", timeZone='" + timeZone + '\'' +
                ", preserveHistory=" + preserveHistory +
                '}';
    }
}
