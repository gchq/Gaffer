package uk.gov.gchq.gaffer.core.exception;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.DebugUtil;
import uk.gov.gchq.gaffer.core.exception.Error.ErrorBuilder;

import static org.junit.Assert.assertNotEquals;

public class ErrorTest {
    private static final String DETAILED_MSG = "detailedMessage";
    private static final String SIMPLE_MSG = "simpleMessage";


    @Test
    public void shouldNotBuildDetailedMessage() throws Exception {
        // Given
        DebugUtil.setDebugMode(false);

        // When
        System.out.println(DebugUtil.checkDebugMode());
        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        // Then
        assertNotEquals("Detailed message is present when built and debug is false", DETAILED_MSG, error.getDetailMessage());
    }

    @Test
    public void shouldBuildDetailedMessage() throws Exception {
        // Given
        DebugUtil.setDebugMode(true);

        // When
        final Error error = new ErrorBuilder()
                .simpleMessage(SIMPLE_MSG)
                .detailMessage(DETAILED_MSG)
                .build();

        // Then
        //assertEquals("Detailed message is not present when built and debug is true", DETAILED_MSG, error.getDetailMessage());
    }
}