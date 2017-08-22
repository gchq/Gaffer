${HEADER}

${CODE_LINK}

This example delves into more detail on Schemas. As we have seen previously there are 2 main components to schemas: the Elements schema and the Types schema.

It is worth remembering that your schema can be broken up further if required. 
For example if you have a Graph that is made up of 5 different data sources, you may find it easier to develop and maintain by splitting your schema into multiple parts, for example 5 Element Schemas and 1 Types schema.
When you construct your Gaffer graph you must provide all the Schema parts. These will then be merged together to form a 'master' schema for the Graph.

We will talk through some more of the features you can add to your Schemas to make them more readable and to model your data more accurately.


#### Elements schema
The elements schema is designed to be a high level document describing what information your Graph contains - like the different types of edges and entities and the list of properties associated with each.
Essentially this part of the schema should just be a list of all the entities and edges in the graph. 
Edges describe the relationship between a source vertex and a destination vertex. 
Entities describe a vertex. We use the term "element" to mean either and edge or and entity.

When defining an element we must provide a "group". This is a unique string that defines the type of an element.
Groups must be completely unique and cannot be shared between edges and entities.

Edges must have the following:
- source - this is the type of object that will be used as the source vertex in your graph. The value here is a string similar to a property type (it could be the same type as a property type).
- destination - similar to source, it can either be the same or a different type.
- directed - we need to tell Gaffer if the edge is directed or undirected. Currently, the easiest way to do this is to create a type called "true", "false" and define that type in the Type schema as being a boolean with a filter predicate to check the boolean is true or false

Entities must have a vertex field - this is similar to the source and destination fields on an edge.


Edges and Entities can optionally have the following fields:
- description - this is a simple string that should provide some context and explain what the element is.
- parents - this should be an array of parent group names. Note - the parent groups must relate to the same element type as the child, for example an edge cannot have an entity as a parent. Elements can inherit any information from multiple parent elements - fields will be merged/overridden so the order that you defined your parents is important. Any fields that are defined in the child element will also merge or override information taken from the parents.
- properties - Properties are be defined by a map of key-value pairs of property names to property types. The property type should be described in the Types schema.
- groupBy - by default Gaffer will use the element group and it's vertices to group similar elements together in order to aggregate and summarise the elements. This groupBy field allows you to specify extra properties that should be used in addition to the element group and vertices to control when similar elements should be grouped together and summarised.
- visibilityProperty - if you are using visibility properties in your graph, then ensure the sensitive elements have a visibility property and then set this visibilityProperty field to that property name so Gaffer knows to restrict access to this element.
- timestampProperty - if you are using timestamp property in your graph, then set this timestampProperty field to that property name so Gaffer knows to treat that property specially.
- aggregate - this is true by default. If you would like to disable aggregation for this element group set this to false.

These 2 optional fields are for advanced users. They can go in the Elements Schema however, we have split them out into separate Schema files Validation and Aggregation so the logic doesn't complicate the Elements schema.
- validateFunctions - an array of selections and predicates to be applied to an element. This allows you to validate based on multiple properties at once - like check a timestamp property together with a time to live property to check if the element should be aged off. Individual property validation is best done as a validateFunction in the property type definition in Types schema.
- aggregateFunctions - an array of selections and binary operators to be applied to an element. This allows you to aggregate based on multiple properties at once. It is important to note that types of properties (groupBy, non-groupBy, visibility) cannot be aggregated in the same aggregate function. The timestamp property is treated as a non-groupBy property. Individual property aggregation is best done as a aggregateFunction in the property type definition in the Types schema.

Here is an example of an Elements schema

${ELEMENTS_JSON}

Here is the Validation Schema. It contains advanced validation, that is applied to multiple properties within an Element group.

${VALIDATION_JSON}

Here is the Aggregation Schema. It contains advanced aggregation, that is applied to multiple properties within an Element group.
The multi property aggregate function defined here overrides the relevant single property aggregate functions defined in the Types schema.

${AGGREGATION_JSON}


#### Types
All types used in the elements schema must be defined in the types parts of the schema. These Types explain to Gaffer what types of properties to expect and how to deal with them.

For each type you must provide the following information:

- class - this is the Java class of the type

You can optionally provide the following:

- description - a string containing a description of the type
- validateFunctions - an array of predicates that will be executed against every type value to validate it. To improve performance put quicker/low cost functions first in the array.
- aggregateFunction - the aggregate binary operator to use to aggregate/summarise/merge property values of the same type together.
- serialiser - an object that contains a field class which represents the java class of the serialiser to use, and potentially arguments depending on the serialiser. If this is not provided Gaffer will attempt to select an appropriate one for you - this is only available for simple Java types.

Here are some example Types
${TYPES_JSON}


##### Serialisers
Gaffer will automatically choose serialisers for you for some core types.
Where possible you should let Gaffer choose for you, as it will choose the optimal
serialiser for the type and your usage.

For custom types you will need to write your own serialiser. When manually
choosing a serialiser for your schema you will need to take the following into
consideration.

For vertex serialisation and groupBy properties you must choose serialisers that
are consistent. A consistent serialiser will serialise the equal objects into
exactly the same values (bytes). For example the JavaSerialiser and
FreqMapSerialiser are not consistent.

When using an ordered store (a store that implements the ORDERED StoreTrait),
this includes Accumulo and HBase, you need to check whether the serialisers are
ordered.

 - for vertex serialisation you must use a serialiser that is ordered
 - for groupBy properties we recommend using a serialiser that is ordered, however it is not essential. In fact it will not cause any problems at present. In the future we plan to add features that would only be available to you if your groupBy properties are serialised using ordered serialisers.
 - all other properties can be serialised with ordered/unordered serialisers.

#### Full Schema
Once the schema has been loaded into a graph the parent elements are merged into the children for performance reasons. This is what the full schema created from the above example schema parts looks like:

```json
${SCHEMA}
```
