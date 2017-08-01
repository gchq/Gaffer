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
package uk.gov.gchq.gaffer.doc.properties.walkthrough;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;
import uk.gov.gchq.gaffer.doc.walkthrough.WalkthroughStrSubstitutor;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.AvroSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.AdaptedBinaryOperator;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorComposite;
import uk.gov.gchq.koryphe.predicate.AdaptedPredicate;
import uk.gov.gchq.koryphe.predicate.PredicateComposite;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperatorComposite;
import uk.gov.gchq.koryphe.tuple.predicate.IntegerTupleAdaptedPredicate;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicateComposite;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

public abstract class PropertiesWalkthrough extends AbstractWalkthrough {
    private static final Set<Class<?>> SYSTEM_CLASSES = Sets.newHashSet(
            AdaptedPredicate.class,
            PredicateComposite.class,
            IntegerTupleAdaptedPredicate.class,
            TupleAdaptedPredicate.class,
            TupleAdaptedPredicateComposite.class,
            ElementAggregator.class,
            AdaptedBinaryOperator.class,
            BinaryOperatorComposite.class,
            TupleAdaptedBinaryOperator.class,
            TupleAdaptedBinaryOperatorComposite.class,
            ElementFilter.class
    );

    protected static final List<Predicate> PREDICATES = Collections.unmodifiableList(getSubClassInstances(Predicate.class));
    protected static final List<BinaryOperator> AGGREGATE_FUNCTIONS = Collections.unmodifiableList(getSubClassInstances(BinaryOperator.class));
    protected static final List<ToBytesSerialiser> TO_BYTES_SERIALISERS = Collections.unmodifiableList(getSubClassInstances(ToBytesSerialiser.class));
    protected static final List<Serialiser> SERIALISERS = Collections.unmodifiableList(getSerialisers());
    protected static final String AGGREGATORS_KEY = "AGGREGATORS";
    protected static final String PREDICATES_KEY = "PREDICATES";
    protected static final String SERIALISERS_KEY = "SERIALISERS";

    private static List<Serialiser> getSerialisers() {
        final List<Serialiser> serialisers = getSubClassInstances(Serialiser.class);
        serialisers.removeIf(c -> {
            boolean contains = false;
            for (final ToBytesSerialiser serialiser : TO_BYTES_SERIALISERS) {
                if (serialiser.getClass().equals(c.getClass())) {
                    contains = true;
                    break;
                }
            }
            return contains;
        });
        return serialisers;
    }

    private Class<?> propertyType;


    protected PropertiesWalkthrough(final Class<?> propertyType, final String resourcePath, final Class<? extends ElementGenerator> generatorClass) {
        super(propertyType.getSimpleName(), null, resourcePath + "/schema", generatorClass, "doc", "properties");
        this.propertyType = propertyType;
    }

    @Override
    public String walkthrough() throws OperationException {
        if (null != propertyType) {
            cacheLogs = true;
            log("PROPERTY_CLASS", "Properties class: " + propertyType.getName());
            listPredicates(propertyType);
            listAggregators(propertyType);
            listSerialisers(propertyType);
        }
        return super.walkthrough();
    }

    protected boolean listPredicates(final Class<?> clazz) {
        final List<String> validateClasses = new ArrayList<>();
        for (final Predicate function : PREDICATES) {
            try {
                final Signature signature = Signature.getInputSignature(function);
                if (signature.assignable(clazz).isValid()) {
                    validateClasses.add(WalkthroughStrSubstitutor.getJavaDocLink(function.getClass(), false));
                }
            } catch (final Exception e) {
                // just add the function.
                validateClasses.add(WalkthroughStrSubstitutor.getJavaDocLink(function.getClass(), false));
            }
        }
        if (!validateClasses.isEmpty()) {
            log(PREDICATES_KEY, "\nPredicates:");
            log(PREDICATES_KEY, "\n- " + StringUtils.join(validateClasses, "\n- "));
        }
        return !validateClasses.isEmpty();
    }

    protected boolean listAggregators(final Class<?> clazz) {
        final List<String> aggregateClasses = new ArrayList<>();
        for (final BinaryOperator function : AGGREGATE_FUNCTIONS) {
            final Signature signature = Signature.getInputSignature(function);
            if (signature.assignable(clazz).isValid()) {
                aggregateClasses.add(WalkthroughStrSubstitutor.getJavaDocLink(function.getClass(), false));
            }
        }
        if (!aggregateClasses.isEmpty()) {
            log(AGGREGATORS_KEY, "\nAggregators:");
            log(AGGREGATORS_KEY, "\n- " + StringUtils.join(aggregateClasses, "\n- "));
        }
        return !aggregateClasses.isEmpty();
    }

    protected void listSerialisers(final Class<?> clazz) {
        listToBytesSerialisers(clazz);
        listOtherSerialisers(clazz);
    }

    protected boolean listToBytesSerialisers(final Class<?> clazz) {
        final List<Class> toBytesSerialiserClasses = new ArrayList<>();
        for (final Serialiser serialise : TO_BYTES_SERIALISERS) {
            if (serialise.canHandle(clazz)) {
                toBytesSerialiserClasses.add(serialise.getClass());
            }
        }
        if (toBytesSerialiserClasses.contains(JavaSerialiser.class)) {
            if (toBytesSerialiserClasses.contains(AvroSerialiser.class)) {
                if (toBytesSerialiserClasses.size() > 2) {
                    toBytesSerialiserClasses.remove(JavaSerialiser.class);
                    toBytesSerialiserClasses.remove(AvroSerialiser.class);
                }
            } else {
                if (toBytesSerialiserClasses.size() > 1) {
                    toBytesSerialiserClasses.remove(JavaSerialiser.class);
                }
            }
        } else if (toBytesSerialiserClasses.contains(AvroSerialiser.class)) {
            if (toBytesSerialiserClasses.size() > 1) {
                toBytesSerialiserClasses.remove(AvroSerialiser.class);
            }
        }
        final List<String> toBytesSerialiserClassNames = new ArrayList<>();
        for (final Class toBytesSerialiserClass : toBytesSerialiserClasses) {
            toBytesSerialiserClassNames.add(WalkthroughStrSubstitutor.getJavaDocLink(toBytesSerialiserClass, false));
        }

        if (!toBytesSerialiserClasses.isEmpty()) {
            log(SERIALISERS_KEY, "\nTo Bytes Serialisers:");
            log(SERIALISERS_KEY, "\n- " + StringUtils.join(toBytesSerialiserClassNames, "\n- "));
        }
        return !toBytesSerialiserClasses.isEmpty();
    }

    protected boolean listOtherSerialisers(final Class<?> clazz) {
        final List<String> serialiserClasses = new ArrayList<>();
        for (final Serialiser serialise : SERIALISERS) {
            if (serialise.canHandle(clazz)) {
                serialiserClasses.add(WalkthroughStrSubstitutor.getJavaDocLink(serialise.getClass(), false));
            }
        }
        if (!serialiserClasses.isEmpty()) {
            log(SERIALISERS_KEY, "\nOther Serialisers:");
            log(SERIALISERS_KEY, "\n- " + StringUtils.join(serialiserClasses, "\n- "));
        }
        return !serialiserClasses.isEmpty();
    }

    private static <T> List<T> getSubClassInstances(final Class<T> clazz) {
        final List<T> instances = new ArrayList<>();
        for (final Class aClass : getSubClasses(clazz)) {
            try {
                instances.add(((Class<T>) aClass).newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                // ignore errors
            }
        }

        return instances;
    }

    private static List<Class> getSubClasses(final Class<?> clazz) {
        final Set<URL> urls = new HashSet<>();
        urls.addAll(ClasspathHelper.forPackage("uk.gov.gchq"));

        final List<Class> classes = new ArrayList<>(new Reflections(urls).getSubTypesOf(clazz));
        keepPublicConcreteClasses(classes);
        classes.removeIf(c -> c.getName().contains("uk.gov.gchq.gaffer.doc"));
        classes.removeAll(SYSTEM_CLASSES);
        classes.sort(Comparator.comparing(Class::getName));

        return classes;

    }

    private static void keepPublicConcreteClasses(final Collection<Class> classes) {
        if (null != classes) {
            final Iterator<Class> itr = classes.iterator();
            for (Class clazz = null; itr.hasNext(); clazz = itr.next()) {
                if (null != clazz) {
                    final int modifiers = clazz.getModifiers();
                    if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers) || Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers)) {
                        itr.remove();
                    }
                }
            }
        }
    }
}
