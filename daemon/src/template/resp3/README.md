# RESP3 Mapping Templates

This directory contains RESP3 mapping templates for the Geodineum Service Daemon (gNode). These templates define how JSON messages are optimized for the RESP3 protocol used in communication with Redis/ValKey.

## Contents

- `field_mapping.json` - Mapping between JSON field names and optimized RESP3 field names
- `redis_mapping.json` - Mapping between JSON data types and Redis RESP3 types

## Usage

The mapping templates are used to:

1. Convert between JSON and RESP3 formats
2. Optimize message size and processing efficiency
3. Define field name shortcuts and command name abbreviations

## Template Format

Each template is a JSON file that defines specific mapping rules:

### Field Mapping Format

The field mapping template includes:

- `commonFields`: Mapping between standard JSON field names and their optimized short versions
- `typeValues`: Mapping between message types and their short codes
- `commandShortNames`: Mapping between command names and their optimized abbreviations

### Redis Mapping Format

The Redis mapping template includes:

- `typeMapping`: Mapping between JSON data types and RESP3 types
- `nativeOptimizations`: Specific optimizations for certain fields

## Extending With Custom Mappings

To create a custom RESP3 mapping:

1. Create a new JSON file in this directory with a `.json` extension
2. Follow the format of existing mapping files
3. Include a `description` field that explains the purpose of the mapping
4. Include a `version` field for tracking changes
5. The file name will be used as the mapping name in the system

Example custom mapping:

```json
{
    "description": "gNode Custom Field Mapping - Optimized field names for specialized commands",
    "version": "1.0.0",
    "specializedCommands": {
        "analytics_query": "an_qry",
        "analytics_report": "an_rep",
        "analytics_dashboard": "an_dash",
        "analytics_metric": "an_met"
    },
    "specializedFields": {
        "query_type": "qt",
        "time_range": "tr",
        "filters": "f",
        "grouping": "g",
        "metrics": "m",
        "dimensions": "d",
        "sort_by": "sb",
        "limit": "l"
    }
}
```