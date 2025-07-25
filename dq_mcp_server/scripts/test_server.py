#!/usr/bin/env python3
"""
Simple test script to verify the MCP server can be imported and basic functions work.
"""

import asyncio
import sys
import os

# Add the src directory to the path so we can import the server
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from mcp_server_dq.server import add


async def test_basic_functionality():
    """Test basic server functionality."""
    print("Testing MCP Server DQ...")
    
    # Test the add function
    result = await add(5, 3)
    print(f"Add test: 5 + 3 = {result}")
    assert result == 8, f"Expected 8, got {result}"
    
    print("✅ Basic functionality test passed!")


if __name__ == "__main__":
    asyncio.run(test_basic_functionality())