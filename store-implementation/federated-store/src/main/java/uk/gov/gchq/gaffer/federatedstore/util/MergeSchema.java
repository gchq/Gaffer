package uk.gov.gchq.gaffer.federatedstore.util;

import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.function.BiFunction;

public class MergeSchema implements BiFunction<Schema, Schema, Schema> {

    @Override
    public Schema apply(final Schema first, final Schema next) {
        return next == null
                ? first
                : new Schema.Builder(first).merge(next).build();
    }
}
