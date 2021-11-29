package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.junit.jupiter.api.Assertions.*;

class ToTimeBucketEndTest extends FunctionTest {
    private static Long SECOND_TIMESTAMPS = Instant.now().getEpochSecond();


    @Test
    void shouldCreateTimeBucketWithSingleTimeInIt() {
        // Given
        final ToTimeBucketEnd toTimeBucketEnd = new ToTimeBucketEnd();
        toTimeBucketEnd.setBucket(CommonTimeUtil.TimeBucket.SECOND);
        // When
        Long result = toTimeBucketEnd.apply(SECOND_TIMESTAMPS);
        long expected = (((((long) Math.ceil(SECOND_TIMESTAMPS)) + 999) / 1000) * 1000)-1;
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
        return new ToTimeBucketEnd();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}