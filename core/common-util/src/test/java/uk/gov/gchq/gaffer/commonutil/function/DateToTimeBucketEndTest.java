package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DateToTimeBucketEndTest extends FunctionTest {
    private static Date CURRENT_DATE =java.util.Calendar.getInstance().getTime();

    @Test
    void shouldConvertDateToEndOfTimeBucket() {
        // Given
        final DateToTimeBucketEnd dateToTimeBucketEnd = new DateToTimeBucketEnd();
        dateToTimeBucketEnd.setBucket(CommonTimeUtil.TimeBucket.DAY);
        // When
        Date result = dateToTimeBucketEnd.apply(CURRENT_DATE);
        long days = TimeUnit.MILLISECONDS.toDays(CURRENT_DATE.getTime());
        long daysToMilliRounded = (days+1)*(1000*60*60*24)-1;
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
        return new DateToTimeBucketEnd();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}