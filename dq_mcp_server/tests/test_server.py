"""Basic tests for the MCP server."""

import pytest
from mcp_server_dq.server import add


@pytest.mark.asyncio
async def test_add_function():
    """Test the basic add function."""
    result = await add(2, 3)
    assert result == 5


@pytest.mark.asyncio
async def test_add_negative_numbers():
    """Test add function with negative numbers."""
    result = await add(-1, 5)
    assert result == 4


@pytest.mark.asyncio
async def test_add_zero():
    """Test add function with zero."""
    result = await add(0, 10)
    assert result == 10