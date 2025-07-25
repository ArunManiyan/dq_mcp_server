#!/usr/bin/env python3
"""
Script to run the MCP server for testing purposes.
This simulates how the server would be run by an MCP client.
"""

import asyncio
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from mcp_server_dq.server import main

if __name__ == "__main__":
    print("Starting MCP Server DQ...")
    print("This server is ready to accept MCP connections via stdio.")
    print("Press Ctrl+C to stop.")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
    except Exception as e:
        print(f"Error running server: {e}")
        sys.exit(1)