package uk.gov.gchq.gaffer.integration.impl.loader;

import org.junit.jupiter.api.TestTemplate;

import uk.gov.gchq.gaffer.store.Store;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
public @interface LoaderTest {
    Class<? extends Store>[] excludeStores() default {};
}
