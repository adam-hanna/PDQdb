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


### Example config.json
```go
{
  "header": [
    {"_id": "string"},
    {"SITE_ADDR": "string"},
    {"TID_INTERP": "string"},
    {"OWNER1": "string"},
    {"UC_INTERP": "string"},
    {"ELEC_UTIL": "string"},
    {"F1_EST_KW": "float32"},
    {"TOTAL_VAL": "int"},
    {"LS_DATE": "string"},
    {"AorC_ZIP": "string"},
    {"OWN_OCC2": "string"},
    {"BLD_AREA": "int"},
    {"NUM_ROOMS": "int"},
    {"CREDIT": "string"},
    {"EST_KWH_CN": "int"},
    {"C_INSTALL1": "string"},
    {"YEAR_BUILT": "string"},
    {"BLDG_AREA_SQFT": "float32"},
    {"EFFCTV_RAT": "float32"},
    {"LATITUDE": "float32"},
    {"LONGITUDE": "float32"},
    {"MAX_BEHAV_SCORE": "int"},
    {"UC_INTERP_SIMP": "string"}
  ],
  "index_field": "_id",
  "start_at_line": 2
}  
```