# Hash table mapper type for Elasticsearch

Allows you to store dictionary in lucene index and then use values in scripts.

## Compilation

```bash
git clone https://github.com/anti-social/htable-java.git
git clone https://github.com/anti-social/elasticsearch-htable-field-mapper.git
cd elasticsearch-htable-field-mapper
git checkout es-2.1
# if you use Elasticsearch 2.0
# git checkout es-2.0
gradle build
```

Zip archive should be into `build/distributions/` directory of the project.

If you want to compile plugin with another version of Elasticsearch you can set `esVersion` property:

```bash
gradle -PesVersion=2.0.0 build
```

## Usage

### Mapping:

```json
{
  "ranks": {
    "type": "htable"
  }
}
```

```json
{
  "ranks": {
    "type": "htable",
    "value_type": "byte"
  }
}
```
#### Mapping options:

`value_type` - type of the stored value. Can be: `byte`, `short`, `int`, `long`, `float` and `double`. Default is `float`.

There are available 2 data formats:

1. `chain` - the default format.

```json
{
  "ranks": {
    "type": "htable",
    "value_type": "byte",
    "format_params": {
      "format": "chain",
      "filling_ratio": 100
    }
  }
}
```

Options for `chain` format:

- `filling_ratio` - specifies minimum number of entries per hash table slot. For example, for `filling_ratio` 10 and number of entries 100 there will be 8 slots. Default is `10`.

- `min_hash_table_size` - minimum number of hash table slots. If calculated number of slots is less than `min_hash_table_size` all the entries will be stored in one sorted list. Default is `2`.

2. `trie` - hash array mapped trie.

```json
{
  "ranks": {
    "type": "htable",
    "value_type": "byte",
    "format_params": {
      "format": "trie",
      "bitmask_size": "short"
    }
  }
}
```

Options for `trie` format:

- `bitmask_size` - specifies number of bits to split keys. For example, `short` means the keys will be split by 4 bits. Available values: `byte`, `short`, `int`, `long`. Default is `short`.

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

There are two scripts: `htable_get` and `htable_get_scale` (only works for byte value type).

`htable_get` gets value from field by specified key:

```json
{
  "function_score": {
    "script_score": {
      "lang": "htable",
      "script": "htable_get",
      "params": {
        "field": "ranks",
        "key": 2
      }
    }
  }
}
```

`htable_get_scale` gets value and scales it in range of [`min_value`, `max_value`]. ONLY for `byte` values.

```json
{
  "function_score": {
    "script_score": {
      "lang": "htable",
      "script": "htable_get_scale",
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
3. https://github.com/anti-social/htable-java
