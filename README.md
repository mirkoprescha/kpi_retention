# spaxploder
SPark based Array eXPLODER - convert array values into separate rows

So you have a dataset with an array-attribute but your BI-Tool can not handle arrays?
The solution is to normalize the data into a relational format in that each array value is transformed into a separate row.
As a result you get a new dataset that contains the array values in one column and additionally the identifier attribute to link back to original dataset.
Spaxploder takes over the transformation utilizing *Apache Spark*, thus datasets of arbitrary size can be transformed.


Example:

This input dataset with an array
```
"_id": 1,
"tags": [
      "incididunt",
      "ullamco",
      "eu",
    ]
"description": "asdi asd basd"
```

will be transformed in that output dataset:
```
"id": 1, "tags":  "incididunt"
"id": 1, "tags":  "ullamco"
"id": 1, "tags":  "eu"
```

## Requirements
- Spark >= 2.0


## Usage
Spaxploder is a spark application intended to be started with `spark-submit` and expects following arguments
```
usage: spaxploder [-h] --input-path INPUT_PATH [--input-fileformat {parquet,json}]
                  --primarykey PRIMARYKEY [--primarykey-datatype PRIMARYKEY_DATATYPE]
                  --array-name ARRAY_NAME
                  [--array-element-datatype ARRAY_ELEMENT_DATATYPE]
                  --output-path OUTPUT_PATH [--output-fileformat {parquet,json}]

SPark Array eXPLODER - convert array values into rows

optional arguments:
  -h, --help             show this help message and exit
  --input-path INPUT_PATH
                         path to input files containing array
  --input-fileformat {parquet,json}
                         fileformat of input files
  --primarykey PRIMARYKEY
                         field name of the id to reference entity of parent row
  --primarykey-datatype PRIMARYKEY_DATATYPE
                         datatype-name of id (e.g. int, long, string)
  --array-name ARRAY_NAME
                         field name of the array that values will be converted into rows
  --array-element-datatype ARRAY_ELEMENT_DATATYPE
                         datatype-name of elements in the  array (e.g. int, long, string,
                         double, boolean)
  --output-path OUTPUT_PATH
                         path to output of exploded array values
  --output-fileformat {parquet,json}
                         fileformat of output
```


To run it as spark-job:
```
spark-submit \
--class com.mirkoprescha.spaxploder.Spaxploder spaxploder-assembly-1.0.jar \
--<parameter 1> <parameter_value1> ...

```


## Features

- supports `parquet` and `json` as input and output fileformat
- transform an array that is inside a struct. Therefore just provide complete attribute-hierarchy as array name (e.g. myStruct.myNumbers)
```
{"id" : 1,  "myStruct":{"myLong" : 1, "myNumbers" : [1,0]}  }
```
- ignore not defined array in an entity.
Thus some entities can have an array some not, no matter if attribute-name is missing are array-value is null.
Spaxploder will ignore these entities without an exception.
- datatype agnostic: just specify the name of the array. Spaxploder will take the values and convert them into rows, whatever datatype it is. Even nested structures are converted.
- supported datatypes: all of spark 2.1



## Options
- enforceSchema (not yet implemented)
- overwrite output (currently default)



## Not yet implemented
- provide programm arguments as configuration file
- unnest array elements that are a StructType -> would need more configuration
```
"friends": [
      {
        "id": 0,
        "name": "Boyd Swanson"
      },
      {
        "id": 1,
        "name": "Reese Whitfield"
      },
      {
        "id": 2,
        "name": "Agnes Winters"
      }
    ],
```
- optionally schema enforcement -> reliable data structures over time
 -> Thus don't overwrite output path with possibly empty dataframe in case of accidently provided wrong array or primary keyname

  - Problems so far with schema
    - Applied schema to dataframe just defines column names and col types that can be used for operations.
    - At same time applying a schema with non-nullable StructType is always handled as nullable.
    - Thus no matter what we define in schema, reading a source file always succeed providing null values for all columns that are not found in file.
    - schema enforcment works for attributes defined in schema that are found in source file. With using "mode" as dataframeReader option the datatype can be enforced.
    - finally we need to implement a way to check if defined attributes are present in file.

- support nested arrays
```
"friends": [
      {
        "id": 0,
        "name": "Sara Key",
        "tags": [
          "non",
          "consectetur",
          "anim"
        ]
      },
      {
        "id": 1,
        "name": "Dyer Lowery",
        "tags": [
          "veniam",
          "est",
          "pariatur"
        ]
      }
    ],
```
- Flatten array as string and reach through of all other attributes.


## compile sources

### Requirements
- Scala
- SBT

```
git clone https://github.com/mirkoprescha/spaxploder.git
sbt assemble
```

## Test spaxploder on local machine with docker
To play around on your local machine you don't need to install spark. With docker you can utilize a docker image with spark that I provide.

Start spark in docker container
```
docker run -it -p 8088:8080   mirkoprescha/spark-zeppelin
```

Copy jar and sample-input json to docker container
```
docker cp src/test/resources/input/json/idArrayOfLong.json $(docker ps  -l -q):/home/
docker cp target/scala-2.11/*.jar $(docker ps  -l -q):/home/
```

Go to docker container and run spaxploder to transform sample inputfile from resources (input/json/idArrayOfLong.json).

```
cd /home

spark-submit \
--class com.mirkoprescha.spaxploder.Spaxploder spaxploder-assembly-1.0.jar \
--input.path idArrayOfLong.json  \
--input.fileformat json          \
--primary.key id                  \
--primary.key.datatype string     \
--array.name myNumbers           \
--array.element.datatype long    \
--output.path /home/output       \
--output.fileformat json
```


## Open decisions
- handling of partitioned output. Just overwrite given partition or config optional output partitions (maybe for first/initial run)


## FAQ
1. What if entity has no array?
These rows will be ignored. Spaxploder will continue with next entity.
2. What if array in entity is null?
As there are no values in array no rows will be created for that entity
