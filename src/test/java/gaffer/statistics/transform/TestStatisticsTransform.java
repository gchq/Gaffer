/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.statistics.transform;

import gaffer.statistics.transform.impl.StatisticsRemoverByName;
import gaffer.utils.WritableToStringConverter;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link StatisticsTransform}. Tests the methods for serialising to string, and
 * deserialising back again.
 */
public class TestStatisticsTransform {

    @Test
    public void testSerialiseDeserialiseToFromString() throws IOException {
        StatisticsTransform transform = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, Collections.singleton("A"));
        String serialised = WritableToStringConverter.serialiseToString(transform);
        StatisticsTransform deserialised = (StatisticsTransform) WritableToStringConverter.deserialiseFromString(serialised);
        assertEquals(transform, deserialised);
    }

}
