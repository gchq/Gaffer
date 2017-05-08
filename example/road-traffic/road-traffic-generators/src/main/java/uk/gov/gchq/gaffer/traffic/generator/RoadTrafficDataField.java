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

package uk.gov.gchq.gaffer.traffic.generator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum RoadTrafficDataField {
    Region_Name,
    ONS_LACode,
    ONS_LA_Name,
    CP,
    S_Ref_E,
    S_Ref_N,
    Road,
    A_Junction,
    A_Ref_E,
    A_Ref_N,
    B_Junction,
    B_Ref_E,
    B_Ref_N,
    RCate,
    iDir,
    Year,
    dCount,
    Hour,
    PC,
    WMV2,
    CAR,
    BUS,
    LGV,
    HGVR2,
    HGVR3,
    HGVR4,
    HGVA3,
    HGVA5,
    HGVA6,
    HGV,
    AMV;

    public static final List<RoadTrafficDataField> VEHICLE_COUNTS = Collections.unmodifiableList(Arrays.asList(PC, WMV2, CAR, BUS, LGV, HGVR2, HGVR3, HGVR4, HGVA3, HGVA5, HGVA6, HGV, AMV));

    public int index() {
        return ordinal();
    }

    public static boolean isHeader(final String line) {
        return line.startsWith("\"Region Name (GO)\",");
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "private method and the null result is handled properly")
    public static String[] extractFields(final String line) {
        if (RoadTrafficDataField.isHeader(line)) {
            return null;
        }

        final String trimStart = StringUtils.removeStart(line, "\"");
        final String trimEnd = StringUtils.removeEnd(trimStart, "\"");
        final String[] fields = trimEnd.split("\",\"");
        if (fields.length != RoadTrafficDataField.values().length) {
            return null;
        }
        return fields;
    }
}
