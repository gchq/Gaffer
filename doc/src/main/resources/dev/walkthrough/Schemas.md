${HEADER}

${CODE_LINK}

This example delves into more detail on Schemas. As we have seen previously there are 3 main components to schemas: the Data Schema, Data Types and Store Types.

It is worth remembering that your schema can be broken up further if required. 
For example if you have a Graph that is made up of 5 different data sources, you may find it easier to develop and maintain by splitting your schema into multiple parts, for example 5 Data Schemas, 1 Data Types and 1 Store Types. 
When you construct your Gaffer graph you must provide all the Schema parts. These will then be merged together to form a 'master' schema for the Graph.

We will talk through some more of the features you can add to your Schemas to make them more readable and to model your data more accurately.


#### Data Schema
The data schema is designed to be a high level document describing what information your Graph contains - like the different types of edges and entities and the list of properties associated with each.
Essentially this part of the schema should just be a list of all the entities and edges in the graph. 
Edges describe the relationship between a source vertex and a destination vertex. 
Entities describe a vertex. We use the term "element" to mean either and edge or and entity.

When defining an element we must provide a "group". This is a unique string that defines the type of an element.
Groups must be completely unique and cannot be shared between edges and entities.

Edges must have the following:
- source - this is the type of object that will be used as the source vertex in your graph. The value here is a string similar to a property type (it could be the same type as a property type).
- destination - similar to source, it can either be the same or a different type.
- directed - we need to tell Gaffer if the edge is directed or undirected. Currently, the easiest way to do this is to create a type called "true", "false" and define that type in the Data Type schema as being a boolean with a filter predicate to check the boolean is true or false

Entities must have a vertex field - this is similar to the source and destination fields on an edge.


Edges and Entities can optionally have the following fields:
- description - this is a simple string that should provide some context and explain what the element is.
- parents - this should be an array of parent group names. Note - the parent groups must relate to the same element type as the child, for example an edge cannot have an entity as a parent. Elements can inherit any information from multiple parent elements - fields will be merged/overridden so the order that you defined your parents is important. Any fields that are defined in the child element will also merge or override information taken from the parents.
- properties - Properties are be defined by a map of key-value pairs of property names to property types. The property type should be described in the Data Types and Store Types schema parts.
- groupBy - by default Gaffer will use the element group and it's vertices to group similar elements together in order to aggregate and summarise the elements. This groupBy field allows you to specify extra properties that should be used in addition to the element group and vertices to control when similar elements should be grouped together and summarised.
- validateFunctions - an array of predicates to be applied to the element as a whole. This allows you to validate based on multiple properties at once - like check a timestamp property together with a time to live property to check if the element should be aged off. Individual property validation is best done as a validateFunction in the property type definition in Data Types.
- visibilityProperty - if you are using visibility properties in your graph, then ensure the sensitive elements have a visibility property and then set this visibilityProperty field to that property name so Gaffer knows to restrict access to this element.
- timestampProperty - if you are using timestamp property in your graph, then set this timestampProperty field to that property name so Gaffer knows to treat that property specially.

Here is an example of a Data Schema
${DATA_SCHEMA_JSON}


#### Types
All types used in the data schema must be defined in the types parts of the schema. We recommend splitting the types into 2 parts, Data Types and Store Types. These Types explain to Gaffer what types of properties to expect and how to deal with them.
The Data Types is simply the Java class of the type and how the type should be validated - e.g. a count has to be more than or equal to 0.

The Store Types contains all the information that the particular Gaffer Store implementation requires to be able to store the data and how to aggregate property values together. We suggest that this is kept separately so if you wish to use the same schema on multiple different Gaffer Stores you only need to write different Store Types files.


##### Data Types
For each type you must provide the following information:

- class - this is the Java class of the type

You can optionally provide the following:

- description - a string containing a description of the type
- validateFunctions - an array of predicates that will be executed against every type value to validate it. To improve performance put quicker/low cost functions first in the array.

Here are some example Data Types
${DATA_TYPES_JSON}

##### Store Types
These fields are optional:

- serialiserClass - the java class of the serialiser to use. If this is not provided Gaffer will attempt to select an appropriate one for you - this is only available for simple Java types.
- aggregateFunction - the aggregate binary operator to use to aggregate/summarise/merge property values of the same type together.

Here are some example Store Types
${STORE_TYPES_JSON}


#### Full Schema
Once the schema has been loaded into a graph the parent elements are merged into the children for performance reasons. This is what the full schema created from the above example schema parts looks like:

```json
${SCHEMA}
```
