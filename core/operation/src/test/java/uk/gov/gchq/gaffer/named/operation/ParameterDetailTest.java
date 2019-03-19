package uk.gov.gchq.gaffer.named.operation;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ParameterDetailTest {

    @Test
    public void shouldBuildFullParameterDetailWithOptions() {
        // When
        List<String> optionsList = Arrays.asList("option1", "option2", "option3");
        new ParameterDetail.Builder()
                .defaultValue(2L)
                .valueClass(Long.class)
                .description("test ParamDetail")
                .required(false)
                .options(optionsList)
                .build();

        // Then - No exceptions
    }
}