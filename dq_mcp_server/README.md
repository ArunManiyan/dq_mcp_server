# MCP Server for AWS Glue Data Quality

A Model Context Protocol (MCP) server that provides tools for managing AWS Glue Data Quality operations. This server enables IDEs and AI assistants to interact with AWS Glue Data Quality services through a standardized interface.

## Features

- **Data Quality Rule Recommendations**: Generate data quality rules automatically based on table schema and data patterns
- **Ruleset Management**: Create, update, and retrieve data quality rulesets for Glue tables
- **Evaluation Runs**: Start and monitor data quality evaluation runs
- **Metrics Retrieval**: Fetch data quality metrics and results from evaluation runs
- **Table Integration**: List and manage rulesets associated with specific Glue tables

## Quick Start

1. Install the server:
   ```bash
   uvx mcp-server-dq
   ```

2. Add to your MCP client configuration (see [Configuration](#configuration) section)

3. Start using the tools in your IDE or AI assistant!

## Installation

### Using uvx (Recommended)

```bash
uvx mcp-server-dq
```

### Using pip

```bash
pip install mcp-server-dq
```

### From source

```bash
git clone https://github.com/yourusername/mcp-server-dq.git
cd mcp-server-dq
uv sync
uv pip install -e .
```

## Configuration

### Prerequisites

1. **AWS Credentials**: Ensure your AWS credentials are configured via:
   - AWS CLI (`aws configure`)
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
   - IAM roles (if running on EC2/ECS)

2. **IAM Permissions**: The following AWS permissions are required:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "glue:GetDataQualityRuleset",
           "glue:CreateDataQualityRuleset",
           "glue:UpdateDataQualityRuleset",
           "glue:ListDataQualityRulesets",
           "glue:StartDataQualityRulesetEvaluationRun",
           "glue:StartDataQualityRuleRecommendationRun",
           "glue:GetDataQualityRulesetEvaluationRun",
           "glue:GetDataQualityRuleRecommendationRun",
           "glue:ListDataQualityRulesetEvaluationRuns",
           "glue:GetDataQualityResult"
         ],
         "Resource": "*"
       }
     ]
   }
   ```

### MCP Client Configuration

Add this server to your MCP client configuration:

#### For Cursor/Claude Desktop

Add to your `mcp.json` configuration file:

```json
{
  "mcpServers": {
    "mcp-server-dq": {
      "command": "uvx",
      "args": ["mcp-server-dq"],
      "env": {
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

#### For other MCP clients

```json
{
  "servers": {
    "mcp-server-dq": {
      "command": "mcp-server-dq",
      "args": [],
      "env": {
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

## Available Tools

### 1. `start_dq_rule_recommendation`
Generate data quality rule recommendations for a Glue table.

**Parameters:**
- `database_name` (str): Glue database name
- `table_name` (str): Table name to analyze
- `ruleset_name` (str): Base name for the generated ruleset
- `wait_for_completion` (bool): Whether to wait for completion (default: true)

### 2. `start_data_quality_ruleset_evaluation_run`
Start a data quality evaluation run using an existing ruleset.

**Parameters:**
- `database_name` (str): Glue database name
- `table_name` (str): Table name to evaluate
- `ruleset_name` (str): Name of the ruleset to use
- `role_arn` (str): IAM role ARN for the evaluation run

### 3. `get_glue_table_rulesets`
Retrieve all data quality rulesets associated with a table.

**Parameters:**
- `database_name` (str): Glue database name
- `table_name` (str): Table name
- `catalog_id` (str, optional): Glue catalog ID

### 4. `list_dq_evaluation_run_results`
Get result IDs from previous evaluation runs.

**Parameters:**
- `table_name` (str): Table name
- `database_name` (str): Database name
- `catalog_id` (str): Catalog ID

### 5. `fetch_dq_metrics`
Fetch detailed metrics from evaluation run results.

**Parameters:**
- `result_id` (list): List of result IDs to fetch

### 6. `add_new_ruleset`
Create a new data quality ruleset for a table.

**Parameters:**
- `database_name` (str): Database name
- `table_name` (str): Table name
- `ruleset_name` (str): Name for the new ruleset
- `catalog_id` (str): Catalog ID
- `ruleset_description` (str): Description of the ruleset
- `rules` (str): DQDL rules content

### 7. `update_rules_to_ruleset`
Append new rules to an existing ruleset.

**Parameters:**
- `ruleset_name` (str): Name of the existing ruleset
- `rules` (str): New rules to append

## Usage Examples

### Generate Data Quality Rules
```
Please generate data quality rules for the table "customer_data" in database "analytics_db"
```

### Run Data Quality Evaluation
```
Start a data quality evaluation for table "orders" in database "sales_db" using ruleset "orders_basic_checks"
```

### Get Table Rulesets
```
Show me all data quality rulesets for table "products" in database "inventory_db"
```

## Development

### Setup Development Environment

```bash
git clone https://github.com/yourusername/mcp-server-dq.git
cd mcp-server-dq
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/
isort src/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:
- Create an issue on [GitHub Issues](https://github.com/yourusername/mcp-server-dq/issues)
- Check the [AWS Glue Data Quality documentation](https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html)

## Changelog

### v0.1.0
- Initial release
- Basic data quality operations support
- MCP server implementation
- AWS Glue integration