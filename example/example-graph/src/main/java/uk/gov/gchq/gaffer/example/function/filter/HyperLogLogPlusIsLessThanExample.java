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
package uk.gov.gchq.gaffer.example.function.filter;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import uk.gov.gchq.gaffer.sketches.function.filter.HyperLogLogPlusIsLessThan;

public class HyperLogLogPlusIsLessThanExample extends FilterFunctionExample {
    private final HyperLogLogPlus hllp1 = new HyperLogLogPlus(15);
    private final HyperLogLogPlus hllp2 = new HyperLogLogPlus(15);
    private final HyperLogLogPlus hllp3 = new HyperLogLogPlus(15);

    public static void main(final String[] args) {
        new HyperLogLogPlusIsLessThanExample().run();
    }

    public HyperLogLogPlusIsLessThanExample() {
        super(HyperLogLogPlusIsLessThan.class);
        hllp1.offer(1);
        hllp2.offer(1);

        hllp2.offer(2);
        hllp3.offer(1);
        hllp3.offer(2);
        hllp3.offer(3);
    }

    public void runExamples() {
        hyperLogLogPlusIsLessThan2();
        hyperLogLogPlusIsLessThanOrEqualTo2();
    }

    public void hyperLogLogPlusIsLessThan2() {
        // ---------------------------------------------------------
        final HyperLogLogPlusIsLessThan function = new HyperLogLogPlusIsLessThan(2);
        // ---------------------------------------------------------

        runExample(function, hllp1, hllp2, hllp3);
    }

    public void hyperLogLogPlusIsLessThanOrEqualTo2() {
        // ---------------------------------------------------------
        final HyperLogLogPlusIsLessThan function = new HyperLogLogPlusIsLessThan(2, true);
        // ---------------------------------------------------------

        runExample(function, hllp1, hllp2, hllp3);
    }
}
