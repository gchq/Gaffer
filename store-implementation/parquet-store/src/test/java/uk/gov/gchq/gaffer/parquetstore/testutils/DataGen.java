/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.testutils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.mutable.WrappedArray$;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public final class DataGen {

    private DataGen() {
        // private to prevent instantiation
    }

    public static Entity getEntity(final String group, final Object vertex, final Byte aByte,
                                   final Float aFloat, final TreeSet<String> treeSet, final Long aLong,
                                   final Short aShort, final Date date, final FreqMap freqMap, final int count, final String visibility) {
        final Entity entity = new Entity(group, vertex);
        entity.putProperty("byte", aByte);
        entity.putProperty("float", aFloat);
        entity.putProperty("treeSet", treeSet);
        entity.putProperty("long", aLong);
        entity.putProperty("short", aShort);
        entity.putProperty("date", date);
        entity.putProperty("freqMap", freqMap);
        entity.putProperty("count", count);
        if (null != visibility) {
            entity.putProperty(TestTypes.VISIBILITY, visibility);
        }
        return entity;
    }

    public static Edge getEdge(final String group, final Object src, final Object dst, final Boolean directed,
                               final Byte aByte, final Float aFloat, final TreeSet<String> treeSet,
                               final Long aLong, final Short aShort, final Date date, final FreqMap freqMap, final int count, final String visibility) {
        final Edge edge = new Edge(group, src, dst, directed);
        edge.putProperty("byte", aByte);
        edge.putProperty("float", aFloat);
        edge.putProperty("treeSet", treeSet);
        edge.putProperty("long", aLong);
        edge.putProperty("short", aShort);
        edge.putProperty("date", date);
        edge.putProperty("freqMap", freqMap);
        edge.putProperty("count", count);
        if (null != visibility) {
            edge.putProperty(TestTypes.VISIBILITY, visibility);
        }
        return edge;
    }

    public static GenericRowWithSchema generateEntityRow(final SchemaUtils utils, final String group, final String vertex,
                                                         final Byte aByte, final Double aDouble, final Float aFloat,
                                                         final TreeSet<String> treeSet, final Long aLong, final Short aShort,
                                                         final Date date, final FreqMap freqMap, final String visibility) throws SerialisationException {
        final GafferGroupObjectConverter entityConverter = new GafferGroupObjectConverter(
                group,
                utils.getCoreProperties(group),
                utils.getCorePropertiesForReversedEdges(),
                utils.getColumnToSerialiser(group),
                utils.getSerialisers(),
                utils.getColumnToPaths(group));
        final List<Object> list = new ArrayList<>();
        final scala.collection.mutable.Map<String, Long> map = new scala.collection.mutable.HashMap<>();
        for (final Map.Entry<String, Long> entry : freqMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects(ParquetStore.VERTEX, vertex)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("byte", aByte)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("double", aDouble)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("float", aFloat)));
        list.add(WrappedArray$.MODULE$.make(entityConverter.gafferObjectToParquetObjects("treeSet", treeSet)[0]));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("long", aLong)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("short", aShort)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("date", date)));
        list.add(map);
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("count", 1)));
        if (null != visibility) {
            list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects(TestTypes.VISIBILITY, visibility)));
        }

        final Object[] objects = new Object[list.size()];
        list.toArray(objects);
        return new GenericRowWithSchema(objects, utils.getSparkSchema(group));
    }

    public static GenericRowWithSchema generateEdgeRow(final SchemaUtils utils, final String group,
                                                       final String src, final String dst, final Boolean directed,
                                                       final Byte aByte, final Double aDouble, final Float aFloat,
                                                       final TreeSet<String> treeSet, final Long aLong, final Short aShort,
                                                       final Date date, final FreqMap freqMap, final String visibility) throws SerialisationException {
        final GafferGroupObjectConverter edgeConverter = new GafferGroupObjectConverter(
                group,
                utils.getCoreProperties(group),
                utils.getCorePropertiesForReversedEdges(),
                utils.getColumnToSerialiser(group),
                utils.getSerialisers(),
                utils.getColumnToPaths(group));
        final List<Object> list = new ArrayList<>();
        final scala.collection.mutable.Map<String, Long> map = new scala.collection.mutable.HashMap<>();
        for (final Map.Entry<String, Long> entry : freqMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStore.SOURCE, src)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStore.DESTINATION, dst)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStore.DIRECTED, directed)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("byte", aByte)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("double", aDouble)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("float", aFloat)));
        list.add(WrappedArray$.MODULE$.make(edgeConverter.gafferObjectToParquetObjects("treeSet", treeSet)[0]));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("long", aLong)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("short", aShort)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("date", date)));
        list.add(map);
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("count", 1)));
        if (null != visibility) {
            list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(TestTypes.VISIBILITY, visibility)));
        }

        final Object[] objects = new Object[list.size()];
        list.toArray(objects);

        return new GenericRowWithSchema(objects, utils.getSparkSchema(group));
    }

    public static List<Element> generateBasicStringEntitysWithNullProperties(final String group, final int size, final boolean withVisibilities) {
        final List<Element> entities = new ArrayList<>();
        String visibility = null;

        if (withVisibilities) {
            visibility = "A";
        }

        for (int x = 0; x < size / 2; x++) {
            final Entity entity = DataGen.getEntity(group, "vert" + x, null, null, null, null, null, null, null, 1, visibility);
            final Entity entity1 = DataGen.getEntity(group, "vert" + x, null, null, null, null, null, null, null, 1, visibility);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicStringEdgesWithNullProperties(final String group, final int size, final boolean withVisibilities) {
        final List<Element> edges = new ArrayList<>();
        String visibility = null;

        if (withVisibilities) {
            visibility = "A";
        }

        for (int x = 0; x < size / 4; x++) {
            final Edge edge = DataGen.getEdge(group, "src" + x, "dst" + x, true, null, null, null, null, null, null, null, 1, visibility);
            final Edge edge1 = DataGen.getEdge(group, "src" + x, "dst" + x, true, null, null, null, null, null, null, null, 1, visibility);
            final Edge edge2 = DataGen.getEdge(group, "src" + x, "dst" + x, false, null, null, null, null, null, null, null, 1, visibility);
            final Edge edge3 = DataGen.getEdge(group, "src" + x, "dst" + x, false, null, null, null, null, null, null, null, 1, visibility);
            edges.add(edge);
            edges.add(edge1);
            edges.add(edge2);
            edges.add(edge3);
        }
        return edges;
    }

    public static List<Element> generateBasicLongEntitys(final String group, final int size, final boolean withVisibilities) {
        final List<Element> entities = new ArrayList<>();
        String visibility = null;

        if (withVisibilities) {
            visibility = "A";
        }

        for (int x = 0; x < size / 2; x++) {
            final Entity entity = DataGen.getEntity(group, (long) x, (byte) 'a', 3f, TestUtils.getTreeSet1(), 5L * x, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility);
            final Entity entity1 = DataGen.getEntity(group, (long) x, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, visibility);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    public static List<Element> generateBasicLongEdges(final String group, final int size, final boolean withVisibilities) {
        final List<Element> edges = new ArrayList<>();
        String visibility = null;

        if (withVisibilities) {
            visibility = "A";
        }

        for (int x = 0; x < size / 4; x++) {
            final Edge edge = DataGen.getEdge(group, (long) x, (long) x + 1, true, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility);
            final Edge edge1 = DataGen.getEdge(group, (long) x, (long) x + 1, true, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, visibility);
            final Edge edge2 = DataGen.getEdge(group, (long) x, (long) x + 1, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility);
            final Edge edge3 = DataGen.getEdge(group, (long) x, (long) x + 1, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, visibility);
            edges.add(edge);
            edges.add(edge1);
            edges.add(edge2);
            edges.add(edge3);
        }
        return edges;
    }

    public static List<Element> generateBasicTypeValueEntitys(final String group, final int size, final boolean withVisibilities) {
        final List<Element> entities = new ArrayList<>();
        String visibility = null;

        if (withVisibilities) {
            visibility = "A";
        }

        for (int x = 0; x < size / 2; x++) {
            final String type = "type" + (x % 5);
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            final Entity entity = DataGen.getEntity(group, vrt, (byte) 'a', 3f, TestUtils.getTreeSet1(), 5L * x, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility);
            final Entity entity1 = DataGen.getEntity(group, vrt, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, visibility);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicTypeValueEdges(final String group, final int size, final boolean withVisibilities) {
        final List<Element> edges = new ArrayList<>();
        String visibility = null;

        if (withVisibilities) {
            visibility = "A";
        }

        for (int x = 0; x < size / 4; x++) {
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final Edge edge = DataGen.getEdge(group, src, dst, true, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility);
            final Edge edge1 = DataGen.getEdge(group, src, dst, true, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, visibility);
            final Edge edge2 = DataGen.getEdge(group, src, dst, false, (byte) 'a', 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility);
            final Edge edge3 = DataGen.getEdge(group, src, dst, false, (byte) 'b', 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, visibility);
            edges.add(edge);
            edges.add(edge1);
            edges.add(edge2);
            edges.add(edge3);
        }
        return edges;
    }

    public static List<Element> generate300StringElementsWithNullProperties(final boolean withVisibilities) {
        final List<Element> elements = new ArrayList<>(300);
        elements.addAll(generateBasicStringEntitysWithNullProperties(TestGroups.ENTITY, 50, withVisibilities));
        elements.addAll(generateBasicStringEdgesWithNullProperties(TestGroups.EDGE, 100, withVisibilities));
        elements.addAll(generateBasicStringEntitysWithNullProperties(TestGroups.ENTITY_2, 50, withVisibilities));
        elements.addAll(generateBasicStringEdgesWithNullProperties(TestGroups.EDGE_2, 100, withVisibilities));
        return elements;
    }

    public static List<Element> generate300LongElements(final boolean withVisibilities) {
        final ArrayList<Element> elements = new ArrayList<>(300);
        elements.addAll(generateBasicLongEntitys(TestGroups.ENTITY, 50, withVisibilities));
        elements.addAll(generateBasicLongEdges(TestGroups.EDGE, 100, withVisibilities));
        elements.addAll(generateBasicLongEntitys(TestGroups.ENTITY_2, 50, withVisibilities));
        elements.addAll(generateBasicLongEdges(TestGroups.EDGE_2, 100, withVisibilities));
        return elements;
    }

    public static List<Element> generate300TypeValueElements(final boolean withVisibilities) {
        final ArrayList<Element> elements = new ArrayList<>(300);
        elements.addAll(generateBasicTypeValueEntitys(TestGroups.ENTITY, 50, withVisibilities));
        elements.addAll(generateBasicTypeValueEdges(TestGroups.EDGE, 100, withVisibilities));
        elements.addAll(generateBasicTypeValueEntitys(TestGroups.ENTITY_2, 50, withVisibilities));
        elements.addAll(generateBasicTypeValueEdges(TestGroups.EDGE_2, 100, withVisibilities));
        return elements;
    }

    public static JavaRDD<Element> generate300StringElementsWithNullPropertiesRDD(final JavaSparkContext spark, final boolean withVisibilities) {
        final List<Element> elements = generate300StringElementsWithNullProperties(withVisibilities);
        return spark.parallelize(elements);
    }

    public static JavaRDD<Element> generate300LongElementsRDD(final JavaSparkContext spark, final boolean withVisibilities) {
        final List<Element> elements = generate300LongElements(withVisibilities);
        return spark.parallelize(elements);
    }

    public static JavaRDD<Element> generate300TypeValueElementsRDD(final JavaSparkContext spark, final boolean withVisibilities) {
        final List<Element> elements = generate300TypeValueElements(withVisibilities);
        return spark.parallelize(elements);
    }
}
