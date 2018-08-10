package com.dataparse.server.service.visualization.bookmark_state.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class DateRangeFilter extends IntegerRangeFilter {
    public DateRangeFilter(String field) {
        super(field);
    }

    public DateRangeFilter(String field, Long min, Long max, Long nullCount, FixedDateType fixedDate) {
        super(field, min, max, nullCount);
        this.fixedDate = fixedDate;
    }

    FixedDateType fixedDate;
}
