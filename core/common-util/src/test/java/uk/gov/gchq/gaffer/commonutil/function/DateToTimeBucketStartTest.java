package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DateToTimeBucketStartTest extends FunctionTest {

    private static Date CURRENT_DATE =java.util.Calendar.getInstance().getTime();

    @Test
    void shouldConvertDateToStartOfTimeBucket() {
        // Given
        final DateToTimeBucketStart dateToTimeBucketStart = new DateToTimeBucketStart();
        dateToTimeBucketStart.setBucket(CommonTimeUtil.TimeBucket.DAY);
        // When
        Date result = dateToTimeBucketStart.apply(CURRENT_DATE);
        long days = TimeUnit.MILLISECONDS.toDays(CURRENT_DATE.getTime());
        long daysToMilliRounded = days * (1000*60*60*24);
        Date expected =new Date(new Timestamp(daysToMilliRounded).getTime());
        // Then
        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Date.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{CommonTimeUtil.TimeBucket.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
    }

    @Override
    protected Object getInstance() {
        return new DateToTimeBucketStart();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}