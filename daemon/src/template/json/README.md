# JSON Schema Templates

This directory contains JSON schema templates for the Geodineum Service Daemon (gNode). These schemas define the structure and validation rules for commands and responses in the system.

## Contents

- `command_schema.json` - Schema for command messages
- `response_schema.json` - Schema for response messages

## Usage

The schema templates are used to:

1. Validate incoming commands from clients
2. Format outgoing responses to clients
3. Document the API for integration

## Schema Format

Each schema follows the JSON Schema Draft-07 format and includes:

- Property definitions with types and descriptions
- Required property declarations
- Validation constraints

## Extending With Custom Schemas

To create a custom JSON schema:

1. Create a new JSON file in this directory with a `.json` extension
2. Follow the JSON Schema Draft-07 format
3. Include a `description` field that explains the purpose of the schema
4. Define all required properties and their types
5. The file name will be used as the schema name in the system

Example custom schema:

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "description": "gNode Batch Command Schema - Format for batch command operations",
    "required": ["batch_id", "commands", "site_id", "node_id", "timestamp"],
    "properties": {
        "batch_id": {
            "type": "string",
            "description": "Unique identifier for the batch"
        },
        "commands": {
            "type": "array",
            "description": "List of commands in the batch",
            "items": {
                "type": "object",
                "required": ["id", "command", "parameters"],
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique identifier for the command"
                    },
                    "command": {
                        "type": "string",
                        "description": "Command name to execute"
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Command parameters"
                    },
                    "sequence": {
                        "type": "integer",
                        "description": "Sequence number within the batch"
                    }
                }
            }
        },
        "site_id": {
            "type": "string",
            "description": "Site identifier for namespacing"
        },
        "node_id": {
            "type": "string",
            "description": "Node identifier"
        },
        "timestamp": {
            "type": "number",
            "description": "Unix timestamp in seconds with millisecond precision"
        }
    }
}
```