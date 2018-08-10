package com.dataparse.server.service.visualization.bookmark_state.filter;

import lombok.*;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class DoubleRangeFilter extends NumberRangeFilter<Double> {

    public DoubleRangeFilter(String field){
        super(field);
        this.setListMode(false);
    }

    public DoubleRangeFilter(String field, Double min, Double max, Long nullCount){
        this(field);
        this.min = min;
        this.max = max;
        this.value1 = min;
        this.value2 = max;
        this.nullCount = nullCount;
    }

    Double min;
    Double max;

    Double value1;
    Double value2;
}
