package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ToTimeBucketTest extends FunctionTest<ToTimeBucket> {
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

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // TODO
    }

    @Override
    protected ToTimeBucket getInstance() {
        return new ToTimeBucket();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
