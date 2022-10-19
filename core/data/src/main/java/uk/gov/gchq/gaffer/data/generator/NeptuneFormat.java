/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import org.codehaus.jackson.annotate.JsonIgnore;

import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.function.ParseTime;
import uk.gov.gchq.koryphe.impl.function.ToBoolean;
import uk.gov.gchq.koryphe.impl.function.ToDouble;
import uk.gov.gchq.koryphe.impl.function.ToFloat;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToString;

public class NeptuneFormat extends CsvFormat {

    @Override
    @JsonIgnore
    public String getVertex() {
        return ":ID";
    }

    @Override
    @JsonIgnore
    public String getEntityGroup() {
        return ":LABEL";
    }

    @Override
    @JsonIgnore
    public String getEdgeGroup() {
        return ":TYPE";
    }

    @Override
    @JsonIgnore
    public String getSource() {
        return ":START_ID";
    }

    @Override
    @JsonIgnore
    public String getDestination() {
        return ":END_ID";
    }

    @Override
    public KorypheFunction<?, ?> getStringTransform() {
        return new ToString();
    }

    @Override
    public KorypheFunction<?, ?> getIntTransform() {
        return new ToInteger();
    }

    @Override
    public KorypheFunction<?, ?> getBooleanTransform() {
        return new ToBoolean();
    }

    @Override
    public KorypheFunction<?, ?> getByteTransform() {
        return new ToInteger();
    }

    @Override
    public KorypheFunction<?, ?> getShortTransform() {
        return new ToInteger();
    }

    @Override
    public KorypheFunction<?, ?> getDateTimeTransform() {
        return new ParseTime();
    }

    @Override
    public KorypheFunction<?, ?> getLongTransform() {
        return new ToLong();
    }

    @Override
    public KorypheFunction<?, ?> getFloatTransform() {
        return new ToFloat();
    }

    @Override
    public KorypheFunction<?, ?> getDoubleTransform() {
        return new ToDouble();
    }

    @Override
    public KorypheFunction<?, ?> getCharTransform() {
        return new ToString();
    }

    @Override
    public KorypheFunction<?, ?> getDateTransform() {
        return new ToString();
    }

    @Override
    public KorypheFunction<?, ?> getLocalDateTransform() {
        return new ToString();
    }

    @Override
    public KorypheFunction<?, ?> getPointTransform() {
        return new ToString();
    }

    @Override
    public KorypheFunction<?, ?> getLocalDateTimeTransform() {
        return new ToString();
    }

    @Override
    public KorypheFunction<?, ?> getDurationTransform() {
        return new ToString();
    }
}
