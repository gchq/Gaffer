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

package uk.gov.gchq.gaffer.tinkerpop.generator;


import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.Text.RegexPredicate;

import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Regex;
import uk.gov.gchq.koryphe.impl.predicate.StringContains;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.util.Collection;
import java.util.function.BiPredicate;

public class KoryphePredicateFactory {

    /**
     * Converts a BiPredicate from a HasContainer to a KoryphePredicate
     * that can be used in a Gaffer View
     *
     * @param biPredicate the predicate to convert
     * @param value the value to compare against
     * @return the equivalent KoryphePredicate
     */
    public KoryphePredicate<?> getKoryphePredicate(final BiPredicate<?, ?> biPredicate, final Object value) {
        if (biPredicate instanceof Compare) {
            return getComparePredicate((Compare) biPredicate, value);
        } else if (biPredicate instanceof Contains) {
            return getContainsPredicate((Contains) biPredicate, (Collection) value);
        } else if (biPredicate instanceof Text) {
            return getTextPredicate((Text) biPredicate, (String) value);
        } else if (biPredicate instanceof RegexPredicate) {
            return getRegexPredicate((RegexPredicate) biPredicate);
        }

        return null;
    }

    private KoryphePredicate<?> getComparePredicate(final Compare c, final Object value) {
        switch (c) {
            case eq:
                return new IsEqual(value);
            case neq:
                return new Not<Object>(new IsEqual(value));
            case gt:
                return new IsMoreThan((Comparable<?>) value);
            case gte:
                return new IsMoreThan((Comparable<?>) value, true);
            case lt:
                return new IsLessThan((Comparable<?>) value);
            case lte:
                return new IsLessThan((Comparable<?>) value, true);
            default:
                return null;
        }
    }

    private KoryphePredicate<?> getContainsPredicate(final Contains c, final Collection<Object> value) {
        switch (c) {
            case within:
                return new IsIn(value);
            case without:
                return new Not<Object>(new IsIn(value));
            default:
                return null;
        }
    }

    private KoryphePredicate<?> getTextPredicate(final Text t, final String value) {
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
                return new Not<String>(new StringContains(value));
            default:
                return null;
        }
    }

    private KoryphePredicate<?> getRegexPredicate(final RegexPredicate p) {
        final Regex r = new Regex(p.getPattern());
        return p.isNegate() ? new Not<String>(r) : r;
    }

}
