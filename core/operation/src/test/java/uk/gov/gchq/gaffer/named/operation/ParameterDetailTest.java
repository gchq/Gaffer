package uk.gov.gchq.gaffer.named.operation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ParameterDetailTest {

    @Test
    public void shouldBuildFullParameterDetailWithOptions() {
        //Given
        List options = Arrays.asList("option1", "option2", "option3");

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

    @Test
    public void shouldBuildFullParameterDetailWithOptionsOfDifferentTypes() {
        //Given
        List options = Arrays.asList("option1", 2, true);

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
