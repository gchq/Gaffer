/*
 * Copyright 2019-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.annotation.JsonInclude;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * Creates a new {@link HyperLogLogPlus} instance and initialises it from
 * the given iterable.
 */
@Since("1.21.0")
@JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
@Summary("Creates a new HyperLogLogPlus instance and initialises it from the given iterable")
@Deprecated
public class IterableToHyperLogLogPlus extends KorypheFunction<Iterable<Object>, HyperLogLogPlus> {
    private int p = 5;

    private int sp = 5;

    public IterableToHyperLogLogPlus() {
    }

    public IterableToHyperLogLogPlus(final int p, final int sp) {
        this.p = p;
        this.sp = sp;
    }

    @Override
    public HyperLogLogPlus apply(final Iterable<Object> o) {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(p, sp);
        if (null != o) {
            for (final Object obj : o) {
                if (null != obj) {
                    hllp.offer(obj);
                }
            }
        }
        return hllp;
    }

    public int getP() {
        return p;
    }

    public void setP(final int p) {
        this.p = p;
    }

    public int getSp() {
        return sp;
    }

    public void setSp(final int sp) {
        this.sp = sp;
    }
}
