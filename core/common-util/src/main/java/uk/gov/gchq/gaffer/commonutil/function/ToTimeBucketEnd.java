/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.function;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import static java.util.Objects.isNull;

/**
 * Converts a timestamp into the end of a timestamp bucket, based on a provided
 * {@link CommonTimeUtil.TimeBucket}.
 */
public class ToTimeBucketEnd extends KorypheFunction<Long, Long> {
    private CommonTimeUtil.TimeBucket bucket;

    public ToTimeBucketEnd() {
    }

    public ToTimeBucketEnd(final CommonTimeUtil.TimeBucket bucket) {
        this.bucket = bucket;
    }

    @Override
    public Long apply(final Long time) {
        if (isNull(time)) {
            return null;
        }
        return CommonTimeUtil.timeToBucketEnd(time, bucket);
    }

    public CommonTimeUtil.TimeBucket getBucket() {
        return bucket;
    }

    public void setBucket(final CommonTimeUtil.TimeBucket bucket) {
        this.bucket = bucket;
    }
}
