package com.spring.es.plugin.annotations;

import java.lang.annotation.*;

/**
 * 类描述：属性
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
@Inherited
public @interface Property {

    FieldType type() default FieldType.Auto;

    boolean index() default true;

    DateFormat format() default DateFormat.none;

    String pattern() default "";

    boolean store() default false;

    boolean fielddata() default false;

    String searchAnalyzer() default "";

    String analyzer() default "";

    String normalizer() default "";

    String[] ignoreFields() default {};

    boolean includeInParent() default false;

    String[] copyTo() default {};
}
