# spaxploder
spark based array exploder - convert array values into rows

```
 "_id": "58e29be3b466eeaed61b5712",
"tags": [
      "incididunt",
      "ullamco",
      "eu",
      "ea",
      "aliquip"
    ]
```
## Options
- enforceSchema
- overwrite
- ignoreNoArray

## Features

- supports `parquet` and `json` as fileformat 
- array in struct 
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

## Not yet implemented
- ignore not defined array in an entity. Some entities have an array some not (even not set as null). Spaxploder will ignore these entities without an exeption *to test*
- unnest structs -> would need more configuration
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
- Flatten array as string and reach trough of all other attributes.


## FAQ
1. What if entity has no array?
These rows will be ignored. Spaxploder will continue with next entity.
2. What if array in entity is null?
As there are no values in array no rows will be created for that entity
