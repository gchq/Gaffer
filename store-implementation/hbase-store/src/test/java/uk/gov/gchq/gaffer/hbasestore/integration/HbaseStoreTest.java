package uk.gov.gchq.gaffer.hbasestore.integration;


import org.junit.jupiter.api.extension.ExtendWith;

import uk.gov.gchq.gaffer.integration.extensions.GafferTestContextProvider;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestContextProvider;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({GafferTestContextProvider.class, LoaderTestContextProvider.class})
public @interface HbaseStoreTest {
}
