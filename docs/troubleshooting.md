# Troubleshooting Cortex

- [Cortex Push API](#cortex--push-api)
  - [Too many labels](#too-many-labels)
  - [Label value too long](#label-value-too-long)

## Cortex Push API


### Too many labels

Cortex imposes a limit on the maximum number of labels a series can have. Once this limit is exceeded while sending series to Cortex, the following error message is returned:

```
sample for '[SERIES]' has [NUMBER] label names; limit [NUMBER]
```

The maximum number of labels allowed for a series is controlled by this configuration block:

```
limits:
  # CLI flag: -validation.max-label-names-per-series
  - max_label_names_per_series: 30
```


### Label value too long

Cortex imposes a limit on the maximum length (number of characters) of a label value. Once this limit is exceeded while sending series to Cortex, the following error message is returned:

```
label value too long: [LABEL VALUE] metric [SERIES]
```

The maximum label value size is controlled by this configuration block:

```
limits:
  # CLI flag: -validation.max-length-label-value
  - max_label_value_length: 2048
```
