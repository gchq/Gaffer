/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.serialisation;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.Set;


public class TypeReferenceImpl {
    public static class CountGroups extends TypeReference<GroupCounts> {
    }

    public static class Void extends TypeReference<java.lang.Void> {
    }

    public static class String extends TypeReference<java.lang.String> {
    }

    public static class Long extends TypeReference<java.lang.Long> {
    }

    public static class Object extends TypeReference<java.lang.Object> {
    }

    public static class Element extends TypeReference<Element> {
    }

    public static class Boolean extends TypeReference<java.lang.Boolean> {
    }

    public static class CloseableIterableObj extends
            TypeReference<CloseableIterable<java.lang.Object>> {
    }

    public static class CloseableIterableElement extends
            TypeReference<CloseableIterable<uk.gov.gchq.gaffer.data.element.Element>> {
    }

    public static class CloseableIterableEntitySeed extends
            TypeReference<CloseableIterable<EntitySeed>> {
    }

    public static class Exporter extends TypeReference<uk.gov.gchq.gaffer.operation.impl.export.Exporter> {
    }

    public static class MapExporter extends TypeReference<java.util.LinkedHashMap<java.lang.String, uk.gov.gchq.gaffer.operation.impl.export.Exporter>> {
    }

    public static class Map extends TypeReference<java.util.LinkedHashMap> {
    }

    public static class MapStringSet extends TypeReference<java.util.LinkedHashMap<String, Set<Object>>> {
    }

    public static class Operations extends TypeReference<Set<Class<Operation>>> {
    }

    public static class JobDetail extends TypeReference<uk.gov.gchq.gaffer.jobtracker.JobDetail> {
    }
}
