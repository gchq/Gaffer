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

import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.util.HashMap;
import java.util.LinkedHashMap;

public abstract class CsvFormat {
    public static final String ENTITY_GROUP = "ENTITY_GROUP";
    public static final String EDGE_GROUP = "EDGE_GROUP";
    private static LinkedHashMap<String, String> identifiers = new LinkedHashMap<String, String>();
    private static HashMap<String, KorypheFunction<?, ?>> transforms = new HashMap<String, KorypheFunction<?, ?>>();
    public abstract String getVertex();
    public abstract String getEntityGroup();
    public abstract String getEdgeGroup();
    public abstract String getSource();
    public abstract String getDestination();
    public abstract KorypheFunction<?, ?> getStringTransform();
    public abstract KorypheFunction<?, ?> getIntTransform();
    public abstract KorypheFunction<?, ?> getBooleanTransform();
    public abstract KorypheFunction<?, ?> getByteTransform();
    public abstract KorypheFunction<?, ?> getShortTransform();
    public abstract KorypheFunction<?, ?> getDateTimeTransform();
    public abstract KorypheFunction<?, ?> getLongTransform();
    public abstract KorypheFunction<?, ?> getFloatTransform();
    public abstract KorypheFunction<?, ?> getDoubleTransform();
    public abstract KorypheFunction<?, ?> getCharTransform();
    public abstract KorypheFunction<?, ?> getDateTransform();
    public abstract KorypheFunction<?, ?> getLocalDateTransform();
    public abstract KorypheFunction<?, ?> getPointTransform();
    public abstract KorypheFunction<?, ?> getLocalDateTimeTransform();
    public abstract KorypheFunction<?, ?> getDurationTransform();


    public static LinkedHashMap<String, String> getIdentifiers(final CsvFormat csvFormat) {
        identifiers.put(String.valueOf(IdentifierType.VERTEX), csvFormat.getVertex());
        identifiers.put(ENTITY_GROUP, csvFormat.getEntityGroup());
        identifiers.put(EDGE_GROUP, csvFormat.getEdgeGroup());
        identifiers.put(String.valueOf(IdentifierType.SOURCE), csvFormat.getSource());
        identifiers.put(String.valueOf(IdentifierType.DESTINATION), csvFormat.getDestination());
        return identifiers;
    }

    public static HashMap<String, KorypheFunction<?, ?> > getTransforms(final CsvFormat csvFormat) {
        transforms.put("String", csvFormat.getStringTransform());
        transforms.put("Char", csvFormat.getStringTransform());
        transforms.put("Date", csvFormat.getStringTransform());
        transforms.put("LocalDate", csvFormat.getStringTransform());
        transforms.put("LocalDateTime", csvFormat.getStringTransform());
        transforms.put("Point", csvFormat.getStringTransform());
        transforms.put("Duration", csvFormat.getStringTransform());
        transforms.put("Int", csvFormat.getIntTransform());
        transforms.put("Short", csvFormat.getIntTransform());
        transforms.put("Byte", csvFormat.getIntTransform());
        transforms.put("DateTime", csvFormat.getDateTimeTransform());
        transforms.put("Long", csvFormat.getLongTransform());
        transforms.put("Double", csvFormat.getDoubleTransform());
        transforms.put("Float", csvFormat.getFloatTransform());
        transforms.put("Boolean", csvFormat.getBooleanTransform());
        return transforms;
    }
}
