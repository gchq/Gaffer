package uk.gov.gchq.gaffer.doc.operation.function;

import uk.gov.gchq.koryphe.tuple.function.KorypheFunction2;

public class ExampleScoreFunction extends KorypheFunction2<Integer, Integer, Integer> {
        @Override
        public Integer apply(final Integer vertex, final Integer count) {
            return vertex * count;
        }
    }