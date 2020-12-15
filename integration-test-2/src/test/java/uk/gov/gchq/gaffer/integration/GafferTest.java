package uk.gov.gchq.gaffer.integration;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import uk.gov.gchq.gaffer.integration.extensions.GafferTestContextProvider;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
@ExtendWith(GafferTestContextProvider.class)
public @interface GafferTest {
}
