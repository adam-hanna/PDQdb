# PDQdb

A read-optimized, in-memory, data processing engine.


## Instructions

1. Install goLang on your computer
2. `$ git clone https://github.com/adam-hanna/PDQdb.git`
3. `$ make`
  * Only builds for linux and MacOS, for now
4. `$ PDQdb -f "path/to/your/file.csv" -c "path/to/your/config.json"`
5. http api (returns json)
  * Grab value by key: `curl -v -XGET http://127.0.0.1:38216/key/{your key}`
  * Count keys: `curl -v -XGET http://127.0.0.1:38216/count`
  * Grab value by index: `curl -X POST  -H "Content-Type: application/json" -d "{\"A\": \"foo\"}" http://localhost:38216/query`


## Example data
###example.csv
```
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