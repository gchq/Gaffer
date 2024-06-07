/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.Text.RegexPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;

import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.impl.predicate.Regex;
import uk.gov.gchq.koryphe.impl.predicate.StringContains;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class KoryphePredicateFactory {

    private static final String COULD_NOT_TRANSLATE_ERROR = "Could not translate Gremlin predicate: ";

    private KoryphePredicateFactory() {
        // Utility class
    }

    /**
     * Converts a Predicate from a HasContainer to a KoryphePredicate
     * that can be used in a Gaffer View
     *
     * @param p the predicate to convert
     * @return the equivalent KoryphePredicate
     */
    public static Predicate<?> getKoryphePredicate(final P<?> p) {
        if (p == null) {
            throw new IllegalArgumentException(COULD_NOT_TRANSLATE_ERROR + null);
        }

        // Handle composite predicates
        if (p instanceof OrP) {
            return getOrPredicate((OrP<?>) p);
        } else if (p instanceof AndP) {
            return getAndPredicate((AndP<?>) p);
        }

        BiPredicate<?, ?> biPredicate = p.getBiPredicate();
        Object value = p.getValue();
        if (biPredicate instanceof Compare) {
            return getComparePredicate((Compare) biPredicate, value);
        } else if (biPredicate instanceof Contains) {
            return getContainsPredicate((Contains) biPredicate, (Collection) value);
        } else if (biPredicate instanceof Text) {
            return getTextPredicate((Text) biPredicate, (String) value);
        } else if (biPredicate instanceof RegexPredicate) {
            return getRegexPredicate((RegexPredicate) biPredicate);
        }

        throw new IllegalArgumentException(COULD_NOT_TRANSLATE_ERROR + p.getPredicateName());
    }

    private static Or<?> getOrPredicate(final OrP<?> orP) {
        List<Predicate> predicates = orP.getPredicates().stream()
                .map(p -> getKoryphePredicate(p))
                .collect(Collectors.toList());

        return new Or<>(predicates);
     }

     private static And<?> getAndPredicate(final AndP<?> andP) {
        List<Predicate> predicates = andP.getPredicates().stream()
                .map(p -> getKoryphePredicate(p))
                .collect(Collectors.toList());

        return new And<>(predicates);
     }

    private static KoryphePredicate<?> getComparePredicate(final Compare c, final Object value) {
        switch (c) {
            case eq:
                return new IsEqual(value);
            case neq:
                return new Not<>(new IsEqual(value));
            case gt:
                return new IsMoreThan((Comparable<?>) value);
            case gte:
                return new IsMoreThan((Comparable<?>) value, true);
            case lt:
                return new IsLessThan((Comparable<?>) value);
            case lte:
                return new IsLessThan((Comparable<?>) value, true);
            default:
                throw new IllegalArgumentException(COULD_NOT_TRANSLATE_ERROR + c.getClass());

        }
    }

    private static KoryphePredicate<?> getContainsPredicate(final Contains c, final Collection<Object> value) {
        switch (c) {
            case within:
                return new IsIn(value);
            case without:
                return new Not<>(new IsIn(value));
            default:
                throw new IllegalArgumentException(COULD_NOT_TRANSLATE_ERROR + c.getClass());

        }
    }

    private static KoryphePredicate<?> getTextPredicate(final Text t, final String value) {
        switch (t) {
            case startingWith:
                return new Regex("^" + value + ".*");
            case notStartingWith:
                return new Regex("^(?!" + value + ").*");
            case endingWith:
                return new Regex(".*" + value + "$");
            case notEndingWith:
                return new Regex(".*(?<!" + value + ")$");
            case containing:
                return new StringContains(value);
            case notContaining:
                return new Not<>(new StringContains(value));
            default:
                throw new IllegalArgumentException(COULD_NOT_TRANSLATE_ERROR + t.getClass());
        }
    }

    private static KoryphePredicate<?> getRegexPredicate(final RegexPredicate p) {
        final Regex r = new Regex(p.getPattern());
        return p.isNegate() ? new Not<>(r) : r;
    }

}
