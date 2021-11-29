package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class ToTimeBucketTest extends FunctionTest {
    private static Long MILLI_TIMESTAMPS = Instant.now().toEpochMilli();

    @Test
    void shouldCreateTimeBucketWithSingleTimeInIt() {
        // Given
        final ToTimeBucket toTimeBucket = new ToTimeBucket();
        toTimeBucket.setBucket(CommonTimeUtil.TimeBucket.MILLISECOND);
        // When
        Long result = toTimeBucket.apply(MILLI_TIMESTAMPS);

        // Then
        assertEquals(MILLI_TIMESTAMPS, result);

    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Long.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Long.class};
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {

    }

    @Override
    protected Object getInstance() {
        return new ToTimeBucket();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}