package com.spring.es.plugin.annotations;

import java.lang.annotation.*;

/**
 * 类描述：多属性
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface MultiField {

    Property mainField();

    InnerField[] otherFields() default {};
}
