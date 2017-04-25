XML Plugin for Apache Drill
====
This plugin allows you to query XML data using Apache Drill.  In theory this will work with XML data of any complexity, however the more nested and complex the data is, the more difficult your queries will be.  Since XML data frequently contains many levels of nesting and some of that nesting is not relevant to the actual data, this plugin allows you to define a "data-level" which is the level of nesting at which Drill will start reading the data.  

Consider the following example data:
```
<books>
  <book>
    <authors>
      <author>Mark Twain</author>
    </authors>
    <title>The Adventures of Tom Sawyer</title>
    <category>FICTION</category>
    <year>1876</year>
  </book>
  <book>
    <authors>
      <author>Niklaus Wirth</author>
    </authors>
    <title>The Programming Language Pascal</title>
    <category>PASCAL</category>
    <year>1971</year>
  </book>
  <book>
    <authors>
      <author>O.-J. Dahl</author>
      <author>E. W. Dijkstra</author>
      <author>C. A. R. Hoare</author>
    </authors>
    <title>Structured Programming</title>
    <category>PROGRAMMING</category>
    <year>1972</year>
  </book>
</books>
```
If this file was read as is, it would first contain a map called "books" which would then contain an array of "book" maps. This is overly complex, so in this plugin, by setting the `data_level` to 2, you can configure Drill to ignore the first two levels of nesting and read this data as if it was a table with columns: authors, title, category and year. 
