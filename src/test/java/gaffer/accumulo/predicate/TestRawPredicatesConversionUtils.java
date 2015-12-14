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
package gaffer.accumulo.predicate;

import gaffer.predicate.Predicate;
import gaffer.predicate.summarytype.impl.SummaryTypeInSetPredicate;
import gaffer.utils.WritableToStringConverter;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test of the serialisation and deserialisation methods in {@link RawGraphElementWithStatisticsPredicate}.
 */
public class TestRawPredicatesConversionUtils {

    @Test
    public void test() throws IOException {
        Predicate<RawGraphElementWithStatistics> predicate = new RawGraphElementWithStatisticsPredicate(new SummaryTypeInSetPredicate("a", "b"));
        String serialisePredicate = WritableToStringConverter.serialiseToString(predicate);
        Predicate<RawGraphElementWithStatistics> read = (Predicate<RawGraphElementWithStatistics>) WritableToStringConverter.deserialiseFromString(serialisePredicate);
        assertEquals(predicate, read);
    }

}
