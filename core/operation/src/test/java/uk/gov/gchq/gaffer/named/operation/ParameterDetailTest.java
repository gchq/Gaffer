package uk.gov.gchq.gaffer.named.operation;

import org.junit.Test;

public class ParameterDetailTest {

    @Test
    public void shouldBuildFullParameterDetail() {
        // When
        new ParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .options("test Option")
                .build();

        // Then - No exceptions
    }
}
