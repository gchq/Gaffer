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

package uk.gov.gchq.gaffer.data.generator;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.data.AlwaysValid;
import uk.gov.gchq.gaffer.data.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.data.Validator;
import uk.gov.gchq.gaffer.data.element.Element;

/**
 * An <code>OneToManyElementGenerator</code> extends {@link ElementGenerator} and provides a one to
 * many generator method for directly converting single domain objects into multiple
 * {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @param <OBJ> the type of domain object
 */
public abstract class OneToManyElementGenerator<OBJ> implements ElementGenerator<OBJ> {
    private Validator<OBJ> objValidator;
    private boolean skipInvalid;

    /**
     * Constructs an <code>OneToManyElementGenerator</code> that doesn't validate the any elements or objects.
     */
    public OneToManyElementGenerator() {
        this(new AlwaysValid<OBJ>(), false);
    }

    /**
     * Constructs an <code>OneToManyElementGenerator</code> with the provided object validator.
     * These validators allow elements and objects to be filtered out before attempting to convert them.
     *
     * @param objValidator a {@link uk.gov.gchq.gaffer.data.Validator} to validate domain objects
     * @param skipInvalid  true if invalid objects should be skipped, otherwise an
     *                     {@link IllegalArgumentException} will be thrown if the validator rejects a value.
     */
    public OneToManyElementGenerator(final Validator<OBJ> objValidator, final boolean skipInvalid) {
        this.objValidator = objValidator;
        this.skipInvalid = skipInvalid;
    }

    public Validator<OBJ> getObjValidator() {
        return objValidator;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public void setObjValidator(final Validator<OBJ> objValidator) {
        this.objValidator = objValidator;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("objValidator")
    Validator<OBJ> getObjValidatorJson() {
        return objValidator instanceof AlwaysValid ? null : objValidator;
    }

    public boolean isSkipInvalid() {
        return skipInvalid;
    }

    public void setSkipInvalid(final boolean skipInvalid) {
        this.skipInvalid = skipInvalid;
    }

    /**
     * @param domainObjects an {@link Iterable} of domain objects to convert
     * @return a {@link uk.gov.gchq.gaffer.data.TransformOneToManyIterable} to lazy convert each domain object into an
     * {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s.
     * @see ElementGenerator#getElements(Iterable)
     */
    @Override
    public Iterable<Element> getElements(final Iterable<OBJ> domainObjects) {
        return new TransformOneToManyIterable<OBJ, Element>(domainObjects, objValidator, skipInvalid) {
            @Override
            protected Iterable<Element> transform(final OBJ item) {
                return getElements(item);
            }
        };
    }

    /**
     * @param domainObject the domain object to convert
     * @return the generated {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s
     */
    public abstract Iterable<Element> getElements(final OBJ domainObject);
}
