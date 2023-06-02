/*
 * Copyright 2022-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class HyperLogLogPlusWithOffers {

    private int p = 5;

    private int sp = 5;

    private byte[] hyperLogLogPlusSketchBytes = null;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    private List<?> offers = new ArrayList<>();

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

    public byte[] getHyperLogLogPlusSketchBytes() {
        return hyperLogLogPlusSketchBytes;
    }

    public void setHyperLogLogPlusSketchBytes(final byte[] hyperLogLogPlusSketchBytes) {
        this.hyperLogLogPlusSketchBytes = hyperLogLogPlusSketchBytes;
    }

    public List<?> getOffers() {
        return offers;
    }

    public void setOffers(final List<?> offers) {
        this.offers = offers;
    }

    @JsonIgnore
    public HyperLogLogPlus getHyperLogLogPlus() throws IOException {
        final HyperLogLogPlus hyperLogLogPlus;
        if (getHyperLogLogPlusSketchBytes() == null) {
            hyperLogLogPlus = new HyperLogLogPlus(getP(), getSp());
        } else {
            hyperLogLogPlus = HyperLogLogPlus.Builder.build(hyperLogLogPlusSketchBytes);
        }

        for (final Object object : getOffers()) {
            hyperLogLogPlus.offer(object);
        }

        return hyperLogLogPlus;
    }
}
