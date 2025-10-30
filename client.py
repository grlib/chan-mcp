import asyncio
from fastmcp import Client

client = Client("http://localhost:8000/mcp")

async def call_tool(symbol: str):
    async with client:
        result = await client.call_tool("chan_signals", {"symbol": symbol})
        print(result)

asyncio.run(call_tool("sh.600000"))