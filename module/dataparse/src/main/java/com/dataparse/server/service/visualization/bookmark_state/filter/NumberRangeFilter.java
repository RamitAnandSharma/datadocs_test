package com.dataparse.server.service.visualization.bookmark_state.filter;

import lombok.*;

import java.util.*;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class NumberRangeFilter<T extends Number> extends Filter {

    Long nullCount;

    public NumberRangeFilter() {
    }

    public NumberRangeFilter(final String field) {
        super(field);
    }

    public abstract T getMin();

    public abstract T getMax();

    public abstract void setMin(T min);

    public abstract void setMax(T max);

    public abstract T getValue1();

    public abstract T getValue2();

    public abstract void setValue1(T min);

    public abstract void setValue2(T max);

    @Override
    public boolean isActive() {
        if(!hidden && selected && !listMode){
            return !Objects.equals(getMin(), getValue1()) || !Objects.equals(getMax(), getValue2());
        }
        return super.isActive();

    }
}
