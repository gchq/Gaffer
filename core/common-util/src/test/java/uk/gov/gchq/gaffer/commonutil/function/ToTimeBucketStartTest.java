package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ToTimeBucketStartTest extends FunctionTest {
    private static Long SECOND_TIMESTAMPS = Instant.now().getEpochSecond();

    @Test
    void shouldCreateTimeBucketWithSingleTimeInIt() {
        // Given
        final ToTimeBucketStart toTimeBucketStart = new ToTimeBucketStart();
        toTimeBucketStart.setBucket(CommonTimeUtil.TimeBucket.SECOND);
        // When
        Long result = toTimeBucketStart.apply(SECOND_TIMESTAMPS);
        long expected = (((long) Math.ceil(SECOND_TIMESTAMPS)) / 1000) * 1000;
        // Then
        assertEquals(expected, result);
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
        return new ToTimeBucketStart();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}