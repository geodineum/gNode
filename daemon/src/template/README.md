# gNode Template Directory

This directory contains schema templates and mapping configurations for the Geodineum Service Daemon (gNode).

## Overview

The template system allows for flexible customization of the gNode's message formats through two main components:

1. **JSON Schema Templates** - Define the structure and validation rules for JSON messages
2. **RESP3 Mapping Templates** - Define the optimization mappings for the RESP3 protocol

## Directory Structure

- `/json` - Contains JSON schema templates
- `/resp3` - Contains RESP3 mapping templates

## JSON Schema Templates

The JSON schema templates define the structure and validation rules for commands and responses in the gNode system. These schemas are used for:

- Validating incoming commands
- Formatting outgoing responses
- Documenting the API

The key schema files are:

- `command_schema.json` - Schema for command messages
- `response_schema.json` - Schema for response messages

## RESP3 Mapping Templates

The RESP3 mapping templates define how JSON structures are mapped to optimized RESP3 protocol format. These mappings are used for:

- Converting between JSON and RESP3 formats
- Optimizing message size and processing efficiency
- Defining field name shortcuts for improved performance

The key mapping files are:

- `field_mapping.json` - Mapping between JSON field names and optimized RESP3 field names
- `redis_mapping.json` - Mapping between JSON data types and Redis RESP3 types

## Using the Templates

Users can create custom templates and place them in the appropriate directories to extend the gNode's capabilities. The system will:

1. Load all templates at startup
2. Validate them for correctness
3. Apply them when processing commands and responses

### Creating Custom Templates

To create a custom template:

1. Copy an existing template as a starting point
2. Modify it to suit your needs
3. Place it in the appropriate directory with a unique name
4. Restart the gNode daemon to load the new template

## Template Format

Templates must be valid JSON with specific structures as defined in the example files. Each template type has its own requirements and validation rules.

## Integration Points

The template system integrates with the gNode in the following ways:

- The command processor uses JSON schemas for validation
- The RESP3 protocol converter uses mapping templates for optimization
- The ValKey functions use mapping templates for protocol conversion

## Example Usage

```rust
// Load a JSON schema template
let command_schema = load_json_schema("command_schema.json")?;

// Validate a command against the schema
let validation_result = validate_command(command, &command_schema)?;

// Load a RESP3 field mapping template
let field_mapping = load_resp3_mapping("field_mapping.json")?;

// Convert a command to optimized RESP3 format
let optimized = convert_to_resp3(command, &field_mapping)?;
```