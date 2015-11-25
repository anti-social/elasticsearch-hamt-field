# Hash array mapped trie mapper type for Elasticsearch

Allows you to store dictionary in lucene index and then use values in scripts.

## Compilation

```bash
git clone https://github.com/anti-social/hamt-java.git
git clone https://github.com/anti-social/elasticsearch-hamt-field-mapper.git
cd elasticsearch-hamt-field-mapper
gradle build
```

Zip archive should be into `build/distributions/` directory of the project.

## Usage

### Mapping:

```json
{
  "ranks": {
    "type": "hamt"
  }
}
```

```json
{
  "ranks": {
    "type": "hamt",
    "value_type": "byte"
  }
}
```
#### Mapping options:

`value_type` - type of the stored value. Can be: `byte`, `short`, `int`, `long`, `float` and `double`. Default is `float`.

`bitmask_size` - defines prefix size of bits for keys in prefix trie, for example `byte` means 3 bits prefix. Can be: `byte`, `short`, `int`, `long`. Default is `short`.

You cannot specify `index` and `doc_values` options for this type of field.

### Document:

`keys` - list of integers.

```json
{
  "ranks": {
    "keys": [1, 2, 103],
    "values": [1.2, 3.4, 5.6]
  }
}
```

### Script:

There are two scripts: `hamt_get` and `hamt_get_scale` (only works for byte value type).

`hamt_get` gets value from field by specified key:

```json
{
  "function_score": {
    "script_score": {
      "lang": "hamt",
      "script": "hamt_get",
      "params": {
        "field": "ranks",
        "key": 2
      }
    }
  }
}
```

`hamt_get_scale` gets value and scales it in range of [`min_value`, `max_value`]. ONLY for `byte` values.

```json
{
  "function_score": {
    "script_score": {
      "lang": "hamt",
      "script": "hamt_get_scale",
      "params": {
        "field": "ranks",
        "key": 1,
        "min_value": 0.85,
        "max_value": 1.5
      }
    }
  }
}
```

## Links

1. https://idea.popcount.org/2012-07-25-introduction-to-hamt/
2. http://lampwww.epfl.ch/papers/idealhashtrees.pdf
3. https://github.com/anti-social/hamt-java
