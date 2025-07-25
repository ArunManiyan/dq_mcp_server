#!/usr/bin/env python3
"""
MCP Server for AWS Glue Data Quality operations.

This server provides tools for managing AWS Glue Data Quality rulesets,
running evaluations, and getting recommendations.
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

import boto3
from mcp.server.fastmcp import FastMCP
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mcp_server_dq")

# Create MCP server instance
mcp = FastMCP("AWS Glue Data Quality Server")

@mcp.tool(description="Add two numbers together")
async def add(x: int, y: int) -> int:
    """Add two numbers and return the result."""
    logger.info(f"Adding numbers: {x} + {y}")
    result = x + y
    logger.info(f"Addition result: {result}")
    return result

@mcp.tool(description="Start a data quality ruleset evaluation run")
async def start_data_quality_ruleset_evaluation_run(
    database_name: str, 
    table_name: str, 
    ruleset_name: str, 
    role_arn: str = 'arn:aws:iam::200904063326:role/service-role/AWSGlueServiceRole-crawler'
) -> str:
    """
    Starts a data quality ruleset evaluation run for a Glue table.
    
    Args:
        database_name: The name of the Glue database
        table_name: The name of the table to evaluate
        ruleset_name: The name of the ruleset to use for evaluation
        role_arn: The IAM role ARN with Glue permissions
    
    Returns:
        str: Information about the evaluation run
    """
    logger.info(f"Starting data quality ruleset evaluation for {database_name}.{table_name}")
    
    try:
        glue = boto3.client("glue")
        
        response = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={
                "GlueTable": {
                    "DatabaseName": database_name,
                    "TableName": table_name
                }
            },
            Role=role_arn,
            NumberOfWorkers=4,
            Timeout=60,
            RulesetNames=[ruleset_name]
        )
        
        run_id = response.get("RunId")
        logger.info(f"Started evaluation run with ID: {run_id}")
        
        result = {
            "run_id": run_id,
            "status": "STARTED",
            "database": database_name,
            "table": table_name,
            "ruleset": ruleset_name
        }
        
        return json.dumps(result, indent=2)
            
    except Exception as e:
        error_msg = f"Error starting evaluation run: {str(e)}"
        logger.error(error_msg)
        return error_msg

@mcp.tool(description="Start a data quality rule recommendation run for a Glue table")
async def start_dq_rule_recommendation(
    database_name: str, 
    table_name: str, 
    ruleset_name: str, 
    wait_for_completion: bool = True
) -> str:
    """
    Use this tool to generate data quality rule set if the user is providing only table name as input and asking to generate the DQ rules
    Starts a data quality rule recommendation run for a specific Glue table.
    
    Args:
        database_name: The name of the Glue database
        table_name: The name of the table to analyze
        ruleset_name: Base name for the ruleset (timestamp will be appended)
        wait_for_completion: Whether to wait for the run to complete
    
    Returns:
        str: Information about the recommendation run and results if available
    """
    logger.info(f"Starting DQ rule recommendation run for {database_name}.{table_name}")
    
    try:
        glue = boto3.client("glue")
        
        response = glue.start_data_quality_rule_recommendation_run(
            DataSource={
                "GlueTable": {
                    "DatabaseName": database_name,
                    "TableName": table_name
                }
            },
            Role='arn:aws:iam::200904063326:role/service-role/AWSGlueServiceRole-crawler',
            NumberOfWorkers=4,
            Timeout=60,
            CreatedRulesetName=ruleset_name + "_" + time.strftime("%Y%m%d_%H%M%S")
        )
        
        run_id = response.get("RunId")
        logger.info(f"Started recommendation run with ID: {run_id}")
        
        result = {
            "run_id": run_id,
            "status": "STARTED",
            "database": database_name,
            "table": table_name
        }
        
        if wait_for_completion and run_id:
            logger.info("Waiting for recommendation run to complete...")
            status = "STARTING"
            
            max_wait_time = 300  # 5 minutes timeout
            start_time = time.time()
            
            while status in ["STARTING", "RUNNING"] and (time.time() - start_time) < max_wait_time:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                run_details = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
                status = run_details.get("Status")
                logger.info(f"Current status: {status}")
                result["status"] = status
                
                if status == "SUCCEEDED":
                    rules_response = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
                    result["rules"] = rules_response.get("RecommendedRuleset")
                    result["recommendation_count"] = len(result["rules"].splitlines()) if result.get("rules") else 0
                    logger.info(f"Successfully retrieved {result['recommendation_count']} rule recommendations")
                    break
                
                elif status in ["FAILED", "STOPPED", "TIMEOUT"]:
                    result["error"] = run_details.get("ErrorString", "Run failed without specific error message")
                    logger.error(f"Recommendation run failed: {result['error']}")
                    break
            
            if status not in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
                result["status"] = "TIMEOUT_WAITING_FOR_COMPLETION"
                logger.warning("Timed out waiting for recommendation run to complete")
        
        return json.dumps(result, indent=2)
            
    except Exception as e:
        error_msg = f"Error in recommendation run: {str(e)}"
        logger.error(error_msg)
        return error_msg

@mcp.tool(description="This tool fetch different DQ run ids and result ids")
async def list_dq_evaluation_run_results(table_name: str, database_name: str, catalog_id: str) -> List[str]:
    """This tool can be used to fetch the result id for various data quality evalution runs.
    Args:
        database_name: The name of the Glue database
        table_name: The name of the table to analyze
        catalog_id: Id for the catalog where the table and database reside
    
    Returns:
        list: Information about the data quality evaluation runs result ids
    """
    glue = boto3.client("glue")
    logger.info(f"Fetching the runId for the table for table {table_name}, database name {database_name}, catalog id {catalog_id}")
    
    response = glue.list_data_quality_ruleset_evaluation_runs(
        Filter={
            'DataSource': {
                'GlueTable': {
                    'DatabaseName': database_name,
                    'TableName': table_name,
                    'CatalogId': catalog_id,
                }
            }
        }
    )
    
    logger.info("Completed the runId fetch")
    run_ids = []
    result_ids = []
    
    for runs in response['Runs']:
        run_ids.append(runs['RunId'])

    for run in run_ids:
        response = glue.get_data_quality_ruleset_evaluation_run(RunId=run)
        result_ids.extend(response.get('ResultIds', []))
        
    return result_ids

@mcp.tool(description="This tool fetchs data qaulity metrics for a given table")
async def fetch_dq_metrics(result_id: List[str]) -> str:
    """This tool can be used to fetch the data quality metrics for a given table."""
    glue = boto3.client("glue")
    logger.info(f"Starting the result fetch for {result_id}")
    
    all_results = []
    for item in result_id:
        try:
            response = glue.get_data_quality_result(ResultId=item)
            all_results.append({
                "result_id": item,
                "rule_results": response.get('RuleResults', [])
            })
            logger.info(f"Completed the result fetch for {item}")
        except Exception as e:
            logger.error(f"Error fetching result for {item}: {str(e)}")
            all_results.append({
                "result_id": item,
                "error": str(e)
            })
    
    logger.info("Completed the result fetch")
    return json.dumps(all_results, indent=2)

@mcp.tool(description="Get data quality rulesets for a Glue table")
async def get_glue_table_rulesets(database_name: str, table_name: str, catalog_id: Optional[str] = None) -> str:
    """
    Retrieves data quality rulesets associated with a specific Glue table.
    
    Args:
        database_name: The name of the Glue database
        table_name: The name of the table
        catalog_id: Optional catalog ID (defaults to caller's account)
    
    Returns:
        str: JSON string containing the rulesets information
    """
    logger.info(f"Getting data quality rulesets for {database_name}.{table_name}")
    
    try:
        glue = boto3.client("glue")
        
        data_source_filter = {
            'DatabaseName': database_name,
            'TableName': table_name
        }
        
        if catalog_id:
            data_source_filter['CatalogId'] = catalog_id
        
        response = glue.list_data_quality_rulesets(
            Filter={
                'TargetTable': data_source_filter
            }
        )
        
        rulesets = response.get('Rulesets', [])
        logger.info(f"Found {len(rulesets)} rulesets for {database_name}.{table_name}")
        
        result = {
            "database": database_name,
            "table": table_name,
            "ruleset_count": len(rulesets),
            "rulesets": []
        }
        
        for ruleset in rulesets:
            ruleset_info = {
                "name": ruleset.get('Name'),
                "description": ruleset.get('Description'),
                "created_on": str(ruleset.get('CreatedOn')),
                "last_modified": str(ruleset.get('LastModifiedOn'))
            }
            
            try:
                ruleset_detail = glue.get_data_quality_ruleset(Name=ruleset.get('Name'))
                ruleset_info["rules"] = ruleset_detail.get('Ruleset')
                ruleset_info["rule_count"] = len(ruleset_detail.get('Ruleset', '').splitlines()) if ruleset_detail.get('Ruleset') else 0
            except Exception as e:
                logger.warning(f"Could not get details for ruleset {ruleset.get('Name')}: {str(e)}")
                ruleset_info["rules"] = "Error retrieving ruleset content"
            
            result["rulesets"].append(ruleset_info)
        
        return json.dumps(result, indent=2)
            
    except Exception as e:
        error_msg = f"Error getting rulesets: {str(e)}"
        logger.error(error_msg)
        return error_msg

@mcp.tool(description="Appends new rules to an existing ruleset")
async def update_rules_to_ruleset(ruleset_name: str, rules: str) -> str:
    """
    Appends new rules to an existing data quality ruleset. Use this tool to add new rules to an existing ruleset.

    Args:
        ruleset_name: The name of the ruleset to update
        rules: The new rules to add to the ruleset

    Returns:
        str: Success if the rule updated successfully or failed if the operation failed
    """
    logger.info(f"Adding new rules to ruleset {ruleset_name}")

    try:
        glue = boto3.client("glue")

        ruleset_detail = glue.get_data_quality_ruleset(Name=ruleset_name)
        current_rules = ruleset_detail.get('Ruleset', '')

        updated_rules = current_rules[:-2] + ",\n    " + rules + "\n]"

        response = glue.update_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset=updated_rules
        )

        logger.info(f"Successfully updated ruleset {ruleset_name}")
        return "Success"

    except Exception as e:
        error_msg = f"Error updating ruleset: {str(e)}"
        logger.error(error_msg)
        return f"Failed: {error_msg}"

@mcp.tool(description="Adds a new ruleset for a Glue table")
async def add_new_ruleset(
    database_name: str, 
    table_name: str, 
    ruleset_name: str, 
    catalog_id: str, 
    ruleset_description: str, 
    rules: str
) -> str:
    """
    Adds a new data quality ruleset for a specific Glue table.

    Args:
        database_name: The name of the Glue database
        table_name: The name of the table
        catalog_id: The ID of the Glue catalog
        ruleset_name: The name of the new ruleset
        ruleset_description: The description of the new ruleset
        rules: The rules to include in the new ruleset

    Returns:
        str: Success if the rule added successfully or failed if the operation failed
    """
    logger.info(f"Adding new ruleset {ruleset_name} for {database_name}.{table_name}")

    try:
        glue = boto3.client("glue")

        response = glue.create_data_quality_ruleset(
            Name=ruleset_name + "_" + time.strftime("%Y%m%d_%H%M%S"),
            Description=ruleset_description or "Created by MCP server",
            Ruleset=rules,
            TargetTable={
                'TableName': table_name,
                'DatabaseName': database_name,
                'CatalogId': catalog_id
            } 
        )

        logger.info(f"Successfully added new ruleset {ruleset_name}")
        return "Success"

    except Exception as e:
        error_msg = f"Error adding new ruleset: {str(e)}"
        logger.error(error_msg)
        return f"Failed: {error_msg}"

async def main():
    """Main entry point for the MCP server."""
    # Use stdio transport for MCP communication
    async with stdio_server() as (read_stream, write_stream):
        await mcp.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mcp-server-dq",
                server_version="0.1.0",
                capabilities=mcp.get_capabilities(
                    notification_options=None,
                    experimental_capabilities=None,
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())