package uk.gov.gchq.gaffer.named.operation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ParameterDetailTest {

    @Test
    public void shouldBuildFullParameterDetailWithOptions() {
        //Given
        List<String> options = Arrays.asList("option1", "option2", "option3");

        // When
        new ParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .options(options)
                .build();

        // Then - No exceptions
    }
}
