/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.joda.time.DateTime.parse;
import static org.junit.Assert.assertEquals;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket.DAY;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket.HOUR;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket.MONTH;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket.WEEK;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.timeToBucket;

public class CommonTimeUtilTest {

    @Test
    public void shouldCorrectlyPlaceTimestampsIntoBuckets() {
        final long time = parse("2000-01-01T12:34:56.789").getMillis();
        final DateTime dateTime = new DateTime(time, DateTimeZone.UTC);

        assertEquals(parse("2000-01-01T12:00:00.000").getMillis(), timeToBucket(time, HOUR));
        assertEquals(parse("2000-01-01T00:00:00.000").getMillis(),timeToBucket(time, DAY));
        assertEquals(parse("1999-12-27T00:00:00.000").getMillis(),timeToBucket(time, WEEK));
        assertEquals(parse("2000-01-01T00:00:00.000").getMillis(),timeToBucket(time, MONTH));
    }

}
