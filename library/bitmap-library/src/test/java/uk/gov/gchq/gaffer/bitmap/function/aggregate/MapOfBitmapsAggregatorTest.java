package uk.gov.gchq.gaffer.bitmap.function.aggregate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.bitmap.types.MapOfBitmaps;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.io.IOException;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MapOfBitmapsAggregatorTest extends BinaryOperatorTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void aggregatorDealsWithNullInput() {
        MapOfBitmapsAggregator mapOfBitmapsAggregator = new MapOfBitmapsAggregator();
        final MapOfBitmaps state = mapOfBitmapsAggregator.apply(null, null);
        assertNull(state);
    }

    @Test
    public void emptyInputBitmapGeneratesEmptyOutputBitmap() {
        MapOfBitmaps mapOfBitmaps1 = new MapOfBitmaps();
        MapOfBitmaps mapOfBitmaps2 = new MapOfBitmaps();
        MapOfBitmapsAggregator mapOfBitmapsAggregator = new MapOfBitmapsAggregator();
        final MapOfBitmaps result = mapOfBitmapsAggregator.apply(mapOfBitmaps1, mapOfBitmaps2);
        assertEquals(0, result.size());
    }

    @Test
    public void shouldAggregateOverLappingBitmapsWithTheSameKeyAndNotAggregateOverLappingBitmapsWithDifferingKeys() {

        MapOfBitmaps mapOfBitmaps1 = new MapOfBitmaps();
        RoaringBitmap inputBitmap = new RoaringBitmap();
        int input1 = 123298333;
        int input2 = 342903339;
        inputBitmap.add(input1);
        inputBitmap.add(input2);
        mapOfBitmaps1.put("bitMapA", inputBitmap);

        MapOfBitmaps mapOfBitmaps2 = new MapOfBitmaps();
        RoaringBitmap inputBitmap2 = new RoaringBitmap();
        int input3 = 123338333;
        int input4 = 343429339;
        inputBitmap2.add(input3);
        inputBitmap2.add(input4);

        mapOfBitmaps2.put("bitMapB", inputBitmap2);

        MapOfBitmaps mapOfBitmaps3 = new MapOfBitmaps();
        RoaringBitmap inputBitmap3 = new RoaringBitmap();
        int input5 = 123298333;
        int input6 = 345353439;
        inputBitmap3.add(input5);
        inputBitmap3.add(input6);

        mapOfBitmaps3.put("bitMapA", inputBitmap3);

        MapOfBitmaps mapOfBitmaps4 = new MapOfBitmaps();
        RoaringBitmap inputBitmap4 = new RoaringBitmap();
        int input7 = 123338333;
        int input8 = 345353439;
        inputBitmap4.add(input7);
        inputBitmap4.add(input8);

        mapOfBitmaps4.put("bitMapA", inputBitmap4);


        MapOfBitmapsAggregator mapOfBitmapsAggregator = new MapOfBitmapsAggregator();
        MapOfBitmaps result = mapOfBitmapsAggregator.apply(mapOfBitmaps1, mapOfBitmaps2);
        result= mapOfBitmapsAggregator.apply(result, mapOfBitmaps3);
        result= mapOfBitmapsAggregator.apply(result, mapOfBitmaps4);

        assertEquals(2, result.size());
        assertEquals(inputBitmap2, result.get("bitMapB"));
        RoaringBitmap resultBitmap2 = result.get("bitMapB");
        assertEquals(2, resultBitmap2.getCardinality());

        RoaringBitmap resultBitmap1 = result.get("bitMapA");
        assertEquals(4, resultBitmap1.getCardinality());
    }

    @Override
    protected RoaringBitmapAggregator getInstance() {
        return new RoaringBitmapAggregator();
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return RoaringBitmapAggregator.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {

    }
}
