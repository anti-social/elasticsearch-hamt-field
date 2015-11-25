# elasticsearch-hamt-field

Hash array mapped trie field for Elasticsearch

Allows you to store dictionary in lucene index and then use values in scripts.

## Usage

### Mapping examples:

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

`value_type` option can be: byte, short, int, long, float, double. Default is float.

### Document examples:

`keys` - list of integers.

```json
{
  "ranks": {
    "keys": [1, 2, 103],
    "values": [1.2, 3.4, 5.6]
  }
}
```

### Script examples:

There are two scripts: `hamt_get` and `hamt_get_scale` (only works for byte value type).

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
