from server import mcp  # single FastMCP instance

# Import tools to register them via decorators
from tools.market_tools import get_bars, chan_signals, chan_structure  # noqa: F401


if __name__ == "__main__":
    mcp.run(transport="http", port=8000)

