/*
 * Copyright 2016-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

/**
 * Utility class which contains a number of inner classes for different {@link TypeReference}s
 * used by the Gaffer project to denote the output type of an {@link uk.gov.gchq.gaffer.operation.io.Output}.
 *
 * @see uk.gov.gchq.gaffer.operation.io.Output#getOutputTypeReference()
 */
@SuppressWarnings("PMD.UnnecessaryFullyQualifiedName") //False positive, the full names are required
public final class TypeReferenceImpl {

    private TypeReferenceImpl() {
    }

    public static class CountGroups extends TypeReference<GroupCounts> {
    }

    public static class Void extends TypeReference<java.lang.Void> {
    }

    public static class String extends TypeReference<java.lang.String> {
    }

    public static class Long extends TypeReference<java.lang.Long> {
    }

    public static class Integer extends TypeReference<java.lang.Integer> {
    }

    public static class Object extends TypeReference<java.lang.Object> {
    }

    public static class Element extends TypeReference<uk.gov.gchq.gaffer.data.element.Element> {
    }

    public static class Boolean extends TypeReference<java.lang.Boolean> {
    }

    public static class IterableObj extends
            TypeReference<Iterable<?>> {
    }

    public static <T> TypeReference<T> createExplicitT() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    public static <T> TypeReference<Iterable<? extends T>> createIterableT() {
        return (TypeReference) new IterableObj();
    }

    public static <T> TypeReference<Iterable<T>> createIterableExplicitT() {
        return (TypeReference) new IterableObj();
    }

    public static class IterableElement extends TypeReference<Iterable<? extends uk.gov.gchq.gaffer.data.element.Element>> {
    }

    public static class IterableEntityId extends TypeReference<Iterable<? extends EntityId>> {
    }

    public static class Exporter extends TypeReference<uk.gov.gchq.gaffer.operation.export.Exporter> {
    }

    public static class MapExporter
            extends TypeReference<java.util.LinkedHashMap<java.lang.String, uk.gov.gchq.gaffer.operation.export.Exporter>> {
    }

    public static class Map extends TypeReference<java.util.LinkedHashMap> {
    }

    public static class MapStringObject extends TypeReference<java.util.Map<java.lang.String, java.lang.Object>> {
    }

    public static class MapStringString extends TypeReference<java.util.Map<java.lang.String, java.lang.String>> {
    }
    public static class MapStringSet extends TypeReference<java.util.Map<java.lang.String, java.util.Set<java.lang.Object>>> {
    }

    public static class Operations extends TypeReference<Set<Class<uk.gov.gchq.gaffer.operation.Operation>>> {
    }

    public static class JobDetail extends TypeReference<uk.gov.gchq.gaffer.jobtracker.JobDetail> {
    }

    public static class JobDetailIterable extends TypeReference<Iterable<uk.gov.gchq.gaffer.jobtracker.JobDetail>> {
    }

    public static class Stream<T> extends TypeReference<java.util.stream.Stream<T>> {
    }

    public static class Array<T> extends TypeReference<T[]> {
    }

    public static class List<T> extends TypeReference<java.util.List<T>> {
    }

    public static class Set<T> extends TypeReference<java.util.Set<T>> {
    }

    public static class IterableEntitySeed extends TypeReference<Iterable<? extends EntitySeed>> {
    }

    public static class IterableMap extends TypeReference<Iterable<? extends java.util.Map<java.lang.String, java.lang.Object>>> {
    }

    public static class IterableString extends TypeReference<Iterable<? extends java.lang.String>> {
    }

    public static class IterableObject extends TypeReference<Iterable<? extends java.lang.Object>> {
    }

    public static class ListString extends TypeReference<java.util.List<java.lang.String>> {
    }

    public static class IterableIterableEdge extends TypeReference<Iterable<Iterable<Edge>>> {
    }

    public static class IterableEdge extends TypeReference<Iterable<Edge>> {
    }

    public static class IterableListEdge extends TypeReference<Iterable<java.util.List<Edge>>> {
    }

    public static class IterableWalk extends TypeReference<Iterable<Walk>> {
    }

    public static class ValidationResult extends TypeReference<uk.gov.gchq.koryphe.ValidationResult> {

    }
}
