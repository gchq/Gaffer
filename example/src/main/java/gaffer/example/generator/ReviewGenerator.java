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

package gaffer.example.generator;

import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.generator.OneToOneElementGenerator;
import gaffer.example.data.Review;
import gaffer.example.data.schema.Group;
import gaffer.example.data.schema.Property;

public class ReviewGenerator extends OneToOneElementGenerator<Review> {
    @Override
    public Element getElement(final Review review) {
        final Entity entity = new Entity(Group.REVIEW, review.getFilmId());
        entity.putProperty(Property.USER_ID, review.getUserId());
        entity.putProperty(Property.RATING, (long) review.getRating());
        entity.putProperty(Property.COUNT, 1);

        return entity;
    }

    @Override
    public Review getObject(final Element element) {
        if (Group.REVIEW.equals(element.getGroup()) && element instanceof Entity) {
            final int rating = (int) ((long) element.getProperty(Property.RATING) / (int) element.getProperty(Property.COUNT));
            return new Review(((Entity) element).getVertex().toString(),
                    element.getProperty(Property.USER_ID).toString(), rating);
        }

        throw new UnsupportedOperationException("Cannot generate Review object from " + element);
    }
}
