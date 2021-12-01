/*
 * Copyright 2019-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.time.function;

import uk.gov.gchq.gaffer.time.CommonTimeUtil;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.Date;

import static java.util.Objects.isNull;

/**
 * Converts a Date into the end of a timestamp bucket, based on a provided
 * {@link CommonTimeUtil.TimeBucket}.
 */
@Since("1.21.0")
@Summary("Converts a Date into the end of a timestamp bucket, based on a provided TimeBucket")
public class DateToTimeBucketEnd extends KorypheFunction<Date, Date> {
    private CommonTimeUtil.TimeBucket bucket;

    public DateToTimeBucketEnd() {
    }

    public DateToTimeBucketEnd(final CommonTimeUtil.TimeBucket bucket) {
        this.bucket = bucket;
    }

    @Override
    public Date apply(final Date date) {
        if (isNull(date)) {
            return null;
        }
        return new Date(CommonTimeUtil.timeToBucketEnd(date.getTime(), bucket));
    }

    public CommonTimeUtil.TimeBucket getBucket() {
        return bucket;
    }

    public void setBucket(final CommonTimeUtil.TimeBucket bucket) {
        this.bucket = bucket;
    }
}
