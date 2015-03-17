# PDQdb

A read-optimized, in-memory, data processing engine.

## Instructions

1. Install goLang on your computer
2. git clone the repo
3. $ make
  Only builds for linux and MacOS, for now
4. $ PDQdb -f "path/to/your/file.csv" -c "path/to/your/config.json"
  See attached config.json example for our data;
  Also note that the first column of your csv must be a unique id!
5. http api (returns json)
  Grab value by key: curl -v -XGET http://127.0.0.1:38216/key/{your key}
  Count keys: curl -v -XGET http://127.0.0.1:38216/count