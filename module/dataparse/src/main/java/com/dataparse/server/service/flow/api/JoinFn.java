package com.dataparse.server.service.flow.api;

import java.util.function.BiFunction;

public enum JoinFn {
    EQ((l, r) -> {
        return l != null && r != null && l.equals(r);
    });

    private BiFunction<Object, Object, Boolean> fn;

    JoinFn(BiFunction<Object, Object, Boolean> fn){
        this.fn = fn;
    }

    public Boolean execute(Object left, Object right) {
        return fn.apply(left, right);
    }
}
