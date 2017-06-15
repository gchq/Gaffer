package uk.gov.gchq.gaffer.bitmap.function.aggregate;

import uk.gov.gchq.gaffer.bitmap.types.MapOfBitmaps;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

public class MapOfBitmapsAggregator extends KorypheBinaryOperator<MapOfBitmaps> {

    private static final RoaringBitmapAggregator BITMAP_AGGREGATOR = new RoaringBitmapAggregator();

    @Override
    protected MapOfBitmaps _apply(MapOfBitmaps a, MapOfBitmaps b) {
        b.forEach((k, v) -> a.merge(k, v, BITMAP_AGGREGATOR));
        return a;
    }
}
