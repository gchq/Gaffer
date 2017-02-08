${HEADER}

${CODE_LINK}

This example goes into a bit more detail on Schemas. As we have seen previously there are 3 main components to schemas: Data Schema, Data Types and Store Types. We will talk through some more of the features you can add to your Schemas to make them more readable and to model your data more accurately.


### Data Schema
The data schema is designed to be a high level document describing what information your Graph contains - like the different types of edges and the list of properties associated with each. Essentially this part of the schema should just be a list of all the entities and edges in the graph. Edges describe the relationship between a source vertex and a destination vertex. Entities describe a vertex. We use the term "Element" to mean either and Edge or and Entity.

When defining an Element we must provide a "group". This is a unique string that defines the type of Element. Groups must be completely unique and cannot be shared between Edges and Entities.

Edges must have the following:
- source - this is the type of object that will be used as the source vertex in your graph. The value here is just a string similar to a property type (it could actually be the same type as a property type).
- destination - similar to source, it can either be the same or a different type.
- directed - we need to tell Gaffer if the edge is directed or undirected. The easiest way to currently do that is to create a type called "true", "false" and define that type in the Data Type schema as being a boolean with a filter function to check the boolean is true or false

Entities must have a vertex field - again this is similar to the source and destination fields on an Edge.


Edges and Entities can optionally have the following fields:
- description - this is a simple string that should provide some context and explain what the element is.
- parents - this should be an array of parent group names. Note - the parent groups must relate to the same Element type as the child, for example an Edge cannot have an Entity as a parent. Elements can inherit any information from multiple parent elements - fields will be merged/overridden so the order that you defined your parents is important. Any fields that are defined in the child Element will also merge or override information taken from the parents.
- properties - Elements don't actually have to have any properties. These properties should be defined in a map of property name to property type. The property type should then be described in the Data Types and Store Types schema parts.
- groupBy - by default Gaffer will use the Element group and it's vertices to group similar elements together in order to aggregate and summarise the elements. This groupBy field allows you to specify extra properties that should be used in addition to the Element group and vertices to limit when similar elements should be grouped together and summarised.
- validateFunctions - an array of validation functions to be applied to the element as a whole. This allows you to validate based on multiple properties at once - like check a timestamp property together with a time to live property to check if the element should be aged off. Individual property validation is best done as a validateFunction in the property type definition in Data Types.
- visibilityProperty - if you are using visibility properties in your graph, then ensure the sensitive edges have a visibility property (any name) and then set this visibilityProperty field to that property name so Gaffer knows to treat that property specially.
- timestampProperty - if you are using timestamp property in your graph, then set this timestampProperty field to that property name so Gaffer knows to treat that property specially.

Here is an example of a Data Schema
${DATA_SCHEMA_JSON}


### Types
All types used in the data schema must be defined in the types parts of the schema. We recommend splitting the types into 2 parts, Data Types and Store Types. These Types explain to Gaffer what types of properties to expect and how to deal with them.
The Data Types is simply the java class of the type and how the type should be validated - e.g. a count has to be more than or equal to 0.
The Store Types contains all the information that the particular Gaffer Store implementation requires to be able to store the data and how to aggregate property values together. This is kept separate so if you wish to use the same schema on more than 1 different Gaffer Store you only need to write different Store Types files.


#### Data Types
For each type you must provide the following information:

- class - this is the java class of the type

You can optionally provide the following:

- description - a string containing a description of the type
- validateFunctions - an array of functions that will be executed against every type value to validate it. To improve performance put quicker/low cost functions first in the array.

Here are some example Data Types
${DATA_TYPES_JSON}

#### Store Types
These fields are optional:

- serialiserClass - the java class of the serialiser to use. If this is not provided Gaffer will attempt to select an appropriate one for you - this is only available for simple java types.
- aggregateFunction - the aggregate function to use to aggregate/summarise/merge property values of the same type together.

Here are some example Store Types
${STORE_TYPES_JSON}


### Full Schema
Once the schema has been loaded into a graph the parent elements are merged into the children for performance reasons. This is what the full schema created from the above example schema parts looks like:

```json
${SCHEMA}
```
