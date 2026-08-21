"""
MCP Confluent client helpers used by the diagnostic agent. Both real tools
used by this PoC (get-consumer-group-lag, consume-messages) work against a
plain bootstrap_servers connection, unlike get-topic-config/alter-topic-config
which require a Confluent Cloud REST endpoint and aren't used by this
scenario.
"""

import json
import logging
import time

import httpx

from .config import MCP_CONFLUENT_URL

logger = logging.getLogger(__name__)


def _call_mcp_raw(tool_name: str, arguments: dict) -> dict:
    """Call an MCP Confluent tool via HTTP JSON-RPC, returning the raw
    JSON-RPC 'result' object (isError / content / structuredContent)."""
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
            "id": int(time.time() * 1000),
        }
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{MCP_CONFLUENT_URL}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            result = response.json()
            mcp_result = result.get("result", {})
            if mcp_result.get("isError"):
                content = mcp_result.get("content") or []
                text = content[0].get("text", "") if content else ""
                logger.warning(f"MCP Confluent tool '{tool_name}' returned an error: {text}")
                return {}
            return mcp_result
    except Exception as e:
        logger.warning(f"MCP Confluent call failed ({tool_name}): {e}")
        return {}


def get_consumer_group_lag(group: str, topic: str) -> dict:
    """Real MCP call: get-consumer-group-lag. Returns the structuredContent
    payload {groupId, topics: [{topic, partitions: [...]}], totalLag}."""
    mcp_result = _call_mcp_raw("get-consumer-group-lag", {"groupId": group, "topics": [topic]})
    return mcp_result.get("structuredContent") or {}


def extract_partition_lag(lag_payload: dict, topic: str, partition: int) -> dict:
    """Pull the {partition, committedOffset, highWatermark, lag, ...} row for
    one (topic, partition) out of a get_consumer_group_lag() payload."""
    for t in lag_payload.get("topics", []):
        if t.get("topic") == topic:
            for p in t.get("partitions", []):
                if p.get("partition") == partition:
                    return p
    return {}


def consume_messages(topic: str, partition: int, offset: int, count: int) -> list:
    """Real MCP call: consume-messages, seeking to an absolute partition
    offset. The tool's response embeds the JSON message list inside a text
    block rather than structuredContent — parsed out here."""
    mcp_result = _call_mcp_raw(
        "consume-messages",
        {
            "topics": [{"name": topic, "partition": partition, "start": {"offset": str(offset)}}],
            "maxMessages": count,
            "timeoutMs": 5000,
        },
    )
    content = mcp_result.get("content") or []
    text = content[0].get("text", "") if content else ""
    marker = "Consumed messages: "
    idx = text.find(marker)
    if idx == -1:
        return []
    try:
        return json.loads(text[idx + len(marker):])
    except json.JSONDecodeError:
        return []
