/*
 * Copyright 2016-2018 Crown Copyright
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum RoadTrafficDataField {
    Region_Name("Region Name (GO)"),
    ONS_LACode("ONS LACode"),
    ONS_LA_Name("ONS LA Name"),
    CP("CP"),
    S_Ref_E("S Ref E"),
    S_Ref_N("S Ref N"),
    S_Ref_Latitude("S Ref Latitude"),
    S_Ref_Longitude("S Ref Longitude"),
    Road("Road"),
    A_Junction("A-Junction"),
    A_Ref_E("A Ref E"),
    A_Ref_N("A Ref N"),
    B_Junction("B-Junction"),
    B_Ref_E("B Ref E"),
    B_Ref_N("B Ref N"),
    RCate("RCat"),
    iDir("iDir"),
    Year("Year"),
    dCount("dCount"),
    Hour("Hour"),
    PC("PC"),
    WMV2("2WMV"),
    CAR("CAR"),
    BUS("BUS"),
    LGV("LGV"),
    HGVR2("HGVR2"),
    HGVR3("HGVR3"),
    HGVR4("HGVR4"),
    HGVA3("HGVA3"),
    HGVA5("HGVA5"),
    HGVA6("HGVA6"),
    HGV("HGV"),
    AMV("AMV");

    public static final List<RoadTrafficDataField> VEHICLE_COUNTS = Collections.unmodifiableList(Arrays.asList(PC, WMV2, CAR, BUS, LGV, HGVR2, HGVR3, HGVR4, HGVA3, HGVA5, HGVA6, HGV, AMV));

    private final String fieldName;

    RoadTrafficDataField(final String fieldName) {
        this.fieldName = fieldName;
    }

    public String fieldName() {
        return this.fieldName;
    }

}
