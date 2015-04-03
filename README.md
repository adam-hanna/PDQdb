# PDQdb

A read-optimized, in-memory, columnar store database (column-oriented DBMS). It's pretty Damn Quick.


## Instructions

1. Install goLang on your computer
2. `$ git clone https://github.com/adam-hanna/PDQdb.git`
3. `$ make`
  * Only builds for Linux and MacOS, for now
4. `$ PDQdb -f "path/to/your/file.csv" -c "path/to/your/config.json"`
5. http api (returns json)
  * Grab data by key: `curl -v -XGET http://127.0.0.1:38216/key/{your key}`
  * Count keys: `curl -v -XGET http://127.0.0.1:38216/count`
  * Grab data by query: see the section on weeQL

## weeQL
weeQL is the query language written for PDQdb. It was inspired by SQL and implements many of the features of the SQL language; however, it is a very lightweight implementation and does not include all the functionality of SQL. Pronounced "wee" + "quill". Get it? Wee SQL? Tiny SQL?

All weeQL queries are made by sending a json query object using the http POST method to `http://127.0.0.1:38216/query`

The json query object is structured as follows:

```
{
  "SELECT":  [ "COL1", "COL2", ... ], // SITUATIONAL
  "COUNT": "*", // SITUATIONAL
  "WHERE":   { "FIELD1": "VAL1", "FIELD2": "VAL2", ... }, // OPTIONAL
  "GROUP BY": "COL1" // OPTIONAL
}
```

<dl>
  <dt><h3>1. Exporting Data</h3>
  <dd>Exporting data is done with the `"SELECT"` query parameter (omit `"COUNT"` and `"GROUP BY"`)
  <dd><h6>Properties</h6>
  <ul>
    <li>`"SELECT" : [ “FIELD 1”, “FIELD 2”, … ]`: SITUATIONAL. An array of strings that indicates the columns to be returned. Omitted if using `"COUNT"`!</li>
    <li>`"WHERE":   { "FIELD1": "VAL1", "FIELD2": "VAL2", ... }`: a subdocument of filters. Multiple filters are returned as the intersection of data that meet each criteria (i.e. "FIELD1" = "VAL1" AND "FIELD2" = "VAL2"). The `"WHERE"` property supports the <b>`"$OR"`</b> key to return the union of filters. `"$OR": [ { "FIELD1": "VAL1" }, {"FIELD2": "VAL2" } ]` is interpreted as "FIELD1" = "VAL1" OR "FIELD2" = "VAL2"</li>
  </ul>
  <dd><h6>example</h6>
  <dd>
<b>query:</b>
```
{
  SELECT : [ “ID” ],
  WHERE: {
    “A”: “foo”
  }
}
```

<b>yields:</b>
```
[
  {
    "ID": "1"
  },
  {
    "ID": "3"
  },
  {
    "ID": "4"
  }
]
```
  <dt><h3>2. Aggregation</h3>
  <dd>Perform counts on data with query parameters and an optional `"GROUP BY"` command (omit `"SELECT"`)
  <dd><h6>Properties</h6>
  <ul>
    <li>`"COUNT": "*"`: SITUATIONAL. The only value currently supported is "*". Omitted if using `"SELECT"`!</li>
    <li>`"WHERE":   { "FIELD1": "VAL1", "FIELD2": "VAL2", ... }`: a subdocument of filters. Multiple filters are returned as the intersection of data that meet each criteria (i.e. "FIELD1" = "VAL1" AND "FIELD2" = "VAL2"). The `"WHERE"` property supports the <b>`"$OR"`</b> key to return the union of filters. `"$OR": [ { "FIELD1": "VAL1" }, {"FIELD2": "VAL2" } ]` is interpreted as "FIELD1" = "VAL1" OR "FIELD2" = "VAL2"</li>
    <li>`"GROUP BY": "COL1"`: OPTIONAL. Only valid with `"COUNT"` queries. This is the string column name by which to group count results.</li>
  </ul>
  <dd><h6>examples</h6>
  <dd>
<b>query #1:</b>

```
{
  “COUNT”: “*”,
  "WHERE": {
    “A”: “foo”
  }
}
```

<b>yields:</b>

```
{ "COUNT": 3 }
```

<b>query #2 with `"GROUP BY"`</b>

```
{
  “COUNT”: “*”,
  "WHERE": {
      “A”: “foo”
  },
  “GROUP BY”: “B”
}
```

<b>yields:</b>

```
{
  "bar": 0,
  "barbar": 0,
  "barbarbar": 0,
  "test": 1,
  "testtest": 1,
  "testtesttest": 1
}
```

<b>query #3 with `"GROUP BY"` and `"$OR"`:</b>

```
{
  “COUNT”: “*”,
  "WHERE": {
    "A": "foo",
    "$OR": [
      { "B": "testtesttest" },
      { "B": "testtest" }
    ]
  },
  “GROUP BY”: “B”
}
```

<b>yields:</b>

```
{
  "bar": 0,
  "barbar": 0,
  "barbarbar": 0,
  "test": 0,
  "testtest": 1,
  "testtesttest": 1
}
```

## Example data
###example.csv
```
ID,A,B
1,foo,test
2,foobar,testtest
3,foo,testtesttest
4,foo,testtest
5,foobarbar,bar
6,foofoo,barbar
7,foofoo,barbarbar
```

###example_config.json
```go
{
  "header": [
    {"ID": "string"},
    {"A": "string"},
    {"B": "string"}
  ],
  "id_field": "ID",
  "index_fields": ["A", "B"],
  "start_at_line": 2
}  
```
