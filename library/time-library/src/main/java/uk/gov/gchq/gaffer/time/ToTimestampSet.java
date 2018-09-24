/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.time;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.time.Instant;

/**
 * Creates a {@link TimestampSet} and initialises it with a single timestamp.
 */
@Since("1.8.0")
@JsonPropertyOrder(value = {"bucket", "maxSize"}, alphabetic = true)
public class ToTimestampSet extends KorypheFunction<Long, TimestampSet> {
    @Required
    private CommonTimeUtil.TimeBucket bucket;

    private Integer maxSize;

    public ToTimestampSet() {
    }

    public ToTimestampSet(final CommonTimeUtil.TimeBucket bucket) {
        this.bucket = bucket;
    }

    public ToTimestampSet(final CommonTimeUtil.TimeBucket bucket, final Integer maxSize) {
        this.bucket = bucket;
        this.maxSize = maxSize;
    }

    @Override
    public TimestampSet apply(final Long timestamp) {
        final TimestampSet timestampSet;
        if (null == maxSize) {
            timestampSet = new RBMBackedTimestampSet.Builder()
                    .timeBucket(bucket)
                    .build();
        } else {
            timestampSet = new BoundedTimestampSet.Builder()
                    .maxSize(maxSize)
                    .timeBucket(bucket)
                    .build();
        }
        if (null != timestamp) {
            timestampSet.add(Instant.ofEpochMilli(timestamp));
        }
        return timestampSet;
    }
}
