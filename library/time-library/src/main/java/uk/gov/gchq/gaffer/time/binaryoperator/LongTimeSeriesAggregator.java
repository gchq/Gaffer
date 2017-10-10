/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.time.binaryoperator;

import uk.gov.gchq.gaffer.time.LongTimeSeries;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.time.Instant;
import java.util.Map;

/**
 * A {@code LongTimeSeriesAggregator} is a {@link java.util.function.BinaryOperator}
 * that takes in {@link LongTimeSeries}s and aggregates the time series. If both
 * time series contain a value for the same timestamp then the two timestamps
 * are summed.
 */
public class LongTimeSeriesAggregator extends KorypheBinaryOperator<LongTimeSeries> {

    @Override
    protected LongTimeSeries _apply(final LongTimeSeries a, final LongTimeSeries b) {
        if (!b.getTimeBucket().equals(a.getTimeBucket())) {
            throw new RuntimeException("Can't aggregate two LongTimeSeries with different time buckets: "
            + "a had bucket " + a.getTimeBucket() + ", b had bucket " + b.getTimeBucket());
        }
        for (final Map.Entry<Instant, Long> entry : b.getTimeSeries().entrySet()) {
            a.upsert(entry.getKey(), entry.getValue());
        }
        return a;
    }
}
