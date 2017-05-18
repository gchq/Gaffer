package uk.gov.gchq.gaffer.core.exception;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.core.exception.Error.ErrorBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 09/05/2017.
 */
public class ErrorBuilderTest {

    public static final String DM = "dm";
    public static final String DETAIL_MESSAGE_EQUALS_DM = "detailMessage=" + DM;
    public static final String SM = "sm";
    public static final String DETAILED_MESSAGE_WAS_NOT_RETAINED_INT_THE_ERROR_BUILDER = "Detailed message was not retained int the ErrorBuilder";


    @Before
    public void setUp() throws Exception {
        System.clearProperty(Error.DEBUG);
        BasicConfigurator.configure();
    }

    @Test
    public void shouldNotBuildDetailedMessage() throws Exception {
        falseProp();
        final ErrorBuilder errorBuilder = getErrorBuilder();
        assertBuilderHasDetailedMessage(errorBuilder);
        assertNotEquals("Detailed message is present when built and debug is false", DM, errorBuilder.build().getDetailMessage());
    }

    @Test
    public void shouldBuildDetailedMessage() throws Exception {
        trueProp();
        final ErrorBuilder errorBuilder = getErrorBuilder();
        assertBuilderHasDetailedMessage(errorBuilder);
        assertEquals("Detailed message is not present when built and debug is true", DM, errorBuilder.build().getDetailMessage());
    }

    @Test
    public void shouldNotBuildDetailedMessageWithMissingPropertyFlag() throws Exception {
        final ErrorBuilder errorBuilder = getErrorBuilder();
        assertBuilderHasDetailedMessage(errorBuilder);
        assertNotEquals("Detailed message is present when built and debug is false", DM, errorBuilder.build().getDetailMessage());
    }

    @Test
    public void shouldNotBuildDetailedMessageWithIncorrectPropertyFlag() throws Exception {
        final ErrorBuilder errorBuilder = getErrorBuilder();
        assertBuilderHasDetailedMessage(errorBuilder);
        assertNotEquals("Detailed message is present when built and debug is false", DM, errorBuilder.build().getDetailMessage());
    }

    private void assertBuilderHasDetailedMessage(final ErrorBuilder errorBuilder) {
        assertTrue(DETAILED_MESSAGE_WAS_NOT_RETAINED_INT_THE_ERROR_BUILDER, errorBuilder.toString().contains(DETAIL_MESSAGE_EQUALS_DM));
    }

    private ErrorBuilder getErrorBuilder() {
        return new ErrorBuilder().simpleMessage(SM).detailMessage(DM);
    }

    private String trueProp() {
        return System.setProperty(Error.DEBUG, Boolean.TRUE.toString());
    }

    private String falseProp() {
        return System.setProperty(Error.DEBUG, Boolean.FALSE.toString());
    }

    private String wrongProp() {
        return System.setProperty(Error.DEBUG, "wrong");
    }
}