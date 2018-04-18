/*
 * Copyright 2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.serialisation.implementation;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import static org.junit.Assert.fail;

public class MultiSerialiserTest extends ToBytesSerialisationTest<Object> {


    @Override
    public Serialiser<Object, byte[]> getSerialisation() {
        MultiSerialiser multiSerialiser = null;
        try {
            multiSerialiser = new MultiSerialiser();
        } catch (Exception e) {
            fail(e.getMessage());
        }
        return multiSerialiser;
    }

    @Override
    public Pair<Object, byte[]>[] getHistoricSerialisationPairs() {
        Pair[] pairs = new Pair[]{
                new Pair("hello", new byte[]{0, 104, 101, 108, 108, 111}),
                new Pair(" world", new byte[]{0, 32, 119, 111, 114, 108, 100}),
                new Pair("asdfjhasjkdfjkasfh6666ggjasdhjkjkhjkhj23jhkjsdhfjasdhfjasdfhjfshhw8374987189475875829345898w78sd7aasdfjhasjkdfjkasfh6666ggjasdhjkjkhjkhj23jhkjsdhfjasdhfjasdfhjfshhw8374987189475875829345898w78sd7aasdfjhasjkdfjkasfh6666ggjasdhjkjkhjkhj23jhkjsdhfjasdhfjasdfhjfshhw8374987189475875829345898w78sd7aasdfjhasjkdfjkasfh6666ggjasdhjkjkhjkhj23jhkjsdhfjasdhfjasdfhjfshhw8374987189475875829345898w78sd7aasdfjhasjkdfjkasfh6666ggjasdhjkjkhjkhj23jhkjsdhfjasdhfjasdfhjfshhw8374987189475875829345898w78sd7aasdfjhasjkdfjkasfh6666ggjasdhjkjkhjkhj23jhkjsdhfjasdhfjasdfhjfshhw8374987189475875829345898w78sd7a", new byte[]{0, 97, 115, 100, 102, 106, 104, 97, 115, 106, 107, 100, 102, 106, 107, 97, 115, 102, 104, 54, 54, 54, 54, 103, 103, 106, 97, 115, 100, 104, 106, 107, 106, 107, 104, 106, 107, 104, 106, 50, 51, 106, 104, 107, 106, 115, 100, 104, 102, 106, 97, 115, 100, 104, 102, 106, 97, 115, 100, 102, 104, 106, 102, 115, 104, 104, 119, 56, 51, 55, 52, 57, 56, 55, 49, 56, 57, 52, 55, 53, 56, 55, 53, 56, 50, 57, 51, 52, 53, 56, 57, 56, 119, 55, 56, 115, 100, 55, 97, 97, 115, 100, 102, 106, 104, 97, 115, 106, 107, 100, 102, 106, 107, 97, 115, 102, 104, 54, 54, 54, 54, 103, 103, 106, 97, 115, 100, 104, 106, 107, 106, 107, 104, 106, 107, 104, 106, 50, 51, 106, 104, 107, 106, 115, 100, 104, 102, 106, 97, 115, 100, 104, 102, 106, 97, 115, 100, 102, 104, 106, 102, 115, 104, 104, 119, 56, 51, 55, 52, 57, 56, 55, 49, 56, 57, 52, 55, 53, 56, 55, 53, 56, 50, 57, 51, 52, 53, 56, 57, 56, 119, 55, 56, 115, 100, 55, 97, 97, 115, 100, 102, 106, 104, 97, 115, 106, 107, 100, 102, 106, 107, 97, 115, 102, 104, 54, 54, 54, 54, 103, 103, 106, 97, 115, 100, 104, 106, 107, 106, 107, 104, 106, 107, 104, 106, 50, 51, 106, 104, 107, 106, 115, 100, 104, 102, 106, 97, 115, 100, 104, 102, 106, 97, 115, 100, 102, 104, 106, 102, 115, 104, 104, 119, 56, 51, 55, 52, 57, 56, 55, 49, 56, 57, 52, 55, 53, 56, 55, 53, 56, 50, 57, 51, 52, 53, 56, 57, 56, 119, 55, 56, 115, 100, 55, 97, 97, 115, 100, 102, 106, 104, 97, 115, 106, 107, 100, 102, 106, 107, 97, 115, 102, 104, 54, 54, 54, 54, 103, 103, 106, 97, 115, 100, 104, 106, 107, 106, 107, 104, 106, 107, 104, 106, 50, 51, 106, 104, 107, 106, 115, 100, 104, 102, 106, 97, 115, 100, 104, 102, 106, 97, 115, 100, 102, 104, 106, 102, 115, 104, 104, 119, 56, 51, 55, 52, 57, 56, 55, 49, 56, 57, 52, 55, 53, 56, 55, 53, 56, 50, 57, 51, 52, 53, 56, 57, 56, 119, 55, 56, 115, 100, 55, 97, 97, 115, 100, 102, 106, 104, 97, 115, 106, 107, 100, 102, 106, 107, 97, 115, 102, 104, 54, 54, 54, 54, 103, 103, 106, 97, 115, 100, 104, 106, 107, 106, 107, 104, 106, 107, 104, 106, 50, 51, 106, 104, 107, 106, 115, 100, 104, 102, 106, 97, 115, 100, 104, 102, 106, 97, 115, 100, 102, 104, 106, 102, 115, 104, 104, 119, 56, 51, 55, 52, 57, 56, 55, 49, 56, 57, 52, 55, 53, 56, 55, 53, 56, 50, 57, 51, 52, 53, 56, 57, 56, 119, 55, 56, 115, 100, 55, 97, 97, 115, 100, 102, 106, 104, 97, 115, 106, 107, 100, 102, 106, 107, 97, 115, 102, 104, 54, 54, 54, 54, 103, 103, 106, 97, 115, 100, 104, 106, 107, 106, 107, 104, 106, 107, 104, 106, 50, 51, 106, 104, 107, 106, 115, 100, 104, 102, 106, 97, 115, 100, 104, 102, 106, 97, 115, 100, 102, 104, 106, 102, 115, 104, 104, 119, 56, 51, 55, 52, 57, 56, 55, 49, 56, 57, 52, 55, 53, 56, 55, 53, 56, 50, 57, 51, 52, 53, 56, 57, 56, 119, 55, 56, 115, 100, 55, 97}),
                new Pair(15423L, new byte[]{1, -114, 60, 63}),
                new Pair(423L, new byte[]{1, -114, 1, -89})
        };

        return pairs;
    }


}
