package com.dataparse.server.util.hibernate;


import java.lang.annotation.*;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.*;

/**
 * Methods, annotated with this annotation will be called in the given order after the full constuction of Application context (all the beans constructed and the PostProcessors finished to run).
 * Those methods can't have parameters.
 * @author jbaruch
 *
 * @since Aug 7, 2008
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
public @interface PostInitialize {
    /**
     * The order in which this post initializer should run. Default is undefined order.
     * @return Set meaningful number in respect to other initializares if order is important.
     */
    int order() default 0;
}

