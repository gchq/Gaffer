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

package gaffer.data.generator;

import gaffer.data.AlwaysValid;
import gaffer.data.TransformIterable;
import gaffer.data.Validator;
import gaffer.data.element.Element;

/**
 * An <code>OneToOneElementGenerator</code> extends {@link gaffer.data.generator.ElementGenerator} and provides one to
 * one generator methods for directly converting a single {@link gaffer.data.element.Element} to a domain object and
 * vice versa.
 *
 * @param <OBJ> the type of domain object
 */
public abstract class OneToOneElementGenerator<OBJ> implements ElementGenerator<OBJ> {

    private final Validator<Element> elementValidator;
    private final Validator<OBJ> objValidator;
    private final boolean skipInvalid;

    /**
     * Constructs an <code>OneToOneElementGenerator</code> that doesn't validate the any elements or objects.
     */
    public OneToOneElementGenerator() {
        this(new AlwaysValid<Element>(), new AlwaysValid<OBJ>(), false);
    }

    /**
     * Constructs an <code>OneToOneElementGenerator</code> with the provided element and object validators.
     * These validators allow elements and objects to be filtered out before attempting to convert them.
     *
     * @param elementValidator a {@link gaffer.data.Validator} to validate {@link gaffer.data.element.Element}s
     * @param objValidator     a {@link gaffer.data.Validator} to validate domain objects
     * @param skipInvalid      true if invalid elements/objects should be skipped, otherwise an
     *                         {@link java.lang.IllegalArgumentException} will be thrown if a validator rejects a value.
     */
    public OneToOneElementGenerator(final Validator<Element> elementValidator, final Validator<OBJ> objValidator,
                                    final boolean skipInvalid) {
        this.elementValidator = elementValidator;
        this.objValidator = objValidator;
        this.skipInvalid = skipInvalid;
    }

    /**
     * @param domainObjects an {@link java.lang.Iterable} of domain objects to convert
     * @return a {@link gaffer.data.TransformIterable} to lazy convert each domain object into an
     * {@link gaffer.data.element.Element}
     * @see gaffer.data.generator.ElementGenerator#getElements(java.lang.Iterable)
     */
    @Override
    public Iterable<Element> getElements(final Iterable<OBJ> domainObjects) {
        return new TransformIterable<OBJ, Element>(domainObjects, objValidator, skipInvalid) {
            @Override
            protected Element transform(final OBJ item) {
                return getElement(item);
            }
        };
    }

    /**
     * @param elements an {@link java.lang.Iterable} of {@link gaffer.data.element.Element} to convert
     * @return a {@link gaffer.data.TransformIterable} to lazy convert each {@link gaffer.data.element.Element} to
     * domain object
     * @see gaffer.data.generator.ElementGenerator#getObjects(java.lang.Iterable)
     */
    @Override
    public Iterable<OBJ> getObjects(final Iterable<Element> elements) {
        return new TransformIterable<Element, OBJ>(elements, elementValidator, skipInvalid) {
            @Override
            protected OBJ transform(final Element item) {
                return getObject(item);
            }
        };
    }

    /**
     * @param domainObject the domain object to convert
     * @return the generated {@link gaffer.data.element.Element}
     */
    public abstract Element getElement(final OBJ domainObject);

    /**
     * @param element the {@link gaffer.data.element.Element} to convert
     * @return the generated domain object
     */
    public abstract OBJ getObject(Element element);
}
