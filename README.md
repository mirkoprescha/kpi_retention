# spaxploder
SPark based Array eXPLODER - convert array values into separate rows

So you have a dataset with an array-attribute but your BI-Tool can not handle arrays?
The solution is to normalize the data into a relational format in that each array value is transformed into a separate row.
As a result you get a new dataset that contains array values in one column and identifier attribute to link back to original dataset.


Example:

Input Dataset with array
```
"_id": 1,
"tags": [
      "incididunt",
      "ullamco",
      "eu",
    ]
"description": "asdi asd basd"
```

Output Dataset:
```
"id": 1, "tags":  "incididunt"
"id": 1, "tags":  "ullamco"
"id": 1, "tags":  "eu"
```

## Usage
Spaxploder is a spark application intended to be started with `spark-submit`

```
spark-submit


```


## Features

- supports `parquet` and `json` as input and output fileformat
- transform an array that is inside a struct. Therefore just provide complete attribute-hierarchy as array name (e.g. myStruct.myNumbers)
```
{"id" : 1,  "myStruct":{"myLong" : 1, "myNumbers" : [1,0]}  }
```
- ignore not defined array in an entity. Some entities have an array some not (even not set as null). Spaxploder will ignore these entities without an exception

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
    
- datatype agnostic: just specify the name of the array. Spaxploder will take the values and convert them into rows, whatever datatype it is. Even nested structures are converted.
- supported datatypes: all of spark 2.1


## Options
- enforceSchema (not implemented)
- overwrite output (default)


##

## Not yet implemented
- provide parameters as configuration file
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
 -> Thus don't overwrite output path with empty dataframe in case of accidently provided wrong array or primary keyname
- Flatten array as string and reach through of all other attributes.

## Open decisions
- handling of partitioned output. Just overwrite given partition or config optional output partitions (maybe for first/initial run)


## FAQ
1. What if entity has no array?
These rows will be ignored. Spaxploder will continue with next entity.
2. What if array in entity is null?
As there are no values in array no rows will be created for that entity
