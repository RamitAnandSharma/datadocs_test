package com.dataparse.server.service.visualization.bookmark_state.filter;

import lombok.*;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class IntegerRangeFilter extends NumberRangeFilter<Long> {

    public IntegerRangeFilter(String field){
        super(field);
        this.setListMode(false);
    }

    public IntegerRangeFilter(String field, Long min, Long max, Long nullCount){
        this(field);
        this.min = min;
        this.max = max;
        this.value1 = min;
        this.value2 = max;
        this.nullCount = nullCount;
    }

    Long min;
    Long max;

    Long value1;
    Long value2;

    Long nullCount;
}
