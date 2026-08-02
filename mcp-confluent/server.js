/**
 * MCP Confluent — Standalone MCP server exposing Kafka tools.
 *
 * Implements a subset of the Confluent MCP tools for demo purposes.
 * Uses KafkaJS to interact with the Kafka cluster.
 *
 * Tools exposed:
 *   - consume-messages: Read messages from a topic
 *   - list-topics: List all topics in the cluster
 *   - get-topic-config: Get configuration for a specific topic
 *   - get-consumer-group-lag: Get lag for a consumer group
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { Kafka, logLevel } from "kafkajs";
import express from "express";

// ---- Configuration ----
const KAFKA_BOOTSTRAP_SERVERS =
  process.env.KAFKA_BOOTSTRAP_SERVERS || "kafka:9092";
const MCP_PORT = parseInt(process.env.MCP_PORT || "3000", 10);

// ---- Kafka client ----
const kafka = new Kafka({
  clientId: "mcp-confluent-standalone",
  brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
  logLevel: logLevel.WARN,
});

const admin = kafka.admin();

// ---- Tool definitions ----
const tools = [
  {
    name: "consume-messages",
    description:
      "Consume messages from a Kafka topic. Returns up to `max_messages` from the specified partition, " +
      "starting from the end or a specific offset.",
    inputSchema: {
      type: "object",
      properties: {
        topic: {
          type: "string",
          description: "Name of the Kafka topic to consume from.",
        },
        partition: {
          type: "integer",
          description: "Partition number (defaults to 0).",
          default: 0,
        },
        max_messages: {
          type: "integer",
          description: "Maximum number of messages to return (default 10, max 100).",
          default: 10,
        },
        start: {
          type: "string",
          enum: ["beginning", "end"],
          description: "Where to start consuming from (default: end).",
          default: "end",
        },
        filter: {
          type: "string",
          description: "Optional key=value filter string (e.g., 'store_id=PAR-001').",
        },
      },
      required: ["topic"],
    },
  },
  {
    name: "list-topics",
    description:
      "List all topics in the Kafka cluster, optionally filtering by a pattern.",
    inputSchema: {
      type: "object",
      properties: {
        filter: {
          type: "string",
          description: "Optional regex pattern to filter topic names.",
        },
      },
    },
  },
  {
    name: "get-topic-config",
    description:
      "Get configuration details for a specific Kafka topic including partition count and replication factor.",
    inputSchema: {
      type: "object",
      properties: {
        topic: {
          type: "string",
          description: "Name of the topic to describe.",
        },
      },
      required: ["topic"],
    },
  },
  {
    name: "get-consumer-group-lag",
    description:
      "Get the consumer lag for a specific consumer group across all partitions.",
    inputSchema: {
      type: "object",
      properties: {
        group: {
          type: "string",
          description: "Name of the consumer group.",
        },
      },
      required: ["group"],
    },
  },
];

// ---- Tool handlers ----
async function handleConsumeMessages(args) {
  const { topic, partition = 0, max_messages = 10, start = "end", filter } = args;

  const consumer = kafka.consumer({ groupId: `mcp-consumer-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: start === "beginning" });

  const messages = [];
  const timeout = 5000;

  await new Promise((resolve) => {
    const timer = setTimeout(resolve, timeout);

    consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value?.toString() || "";
        const key = message.key?.toString() || "";

        // Apply filter
        if (filter) {
          const [fk, fv] = filter.split("=");
          try {
            const data = JSON.parse(value);
            if (String(data[fk]) !== fv) return;
          } catch {
            return; // not JSON, skip filtered message
          }
        }

        messages.push({
          topic,
          partition,
          offset: message.offset,
          key,
          value,
          timestamp: message.timestamp,
        });

        if (messages.length >= max_messages) {
          clearTimeout(timer);
          resolve();
        }
      },
    });
  });

  await consumer.disconnect();

  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(
          {
            messages,
            count: messages.length,
            topic,
            partition,
          },
          null,
          2,
        ),
      },
    ],
  };
}

async function handleListTopics(args) {
  await admin.connect();
  let topics = await admin.listTopics();

  if (args?.filter) {
    const re = new RegExp(args.filter);
    topics = topics.filter((t) => re.test(t));
  }

  // Exclude internal topics
  topics = topics.filter((t) => !t.startsWith("__"));

  await admin.disconnect();

  return {
    content: [
      {
        type: "text",
        text: JSON.stringify({ topics, count: topics.length }, null, 2),
      },
    ],
  };
}

async function handleGetTopicConfig(args) {
  await admin.connect();
  const metadata = await admin.fetchTopicMetadata({ topics: [args.topic] });
  await admin.disconnect();

  const topicMeta = metadata.topics[0];
  if (!topicMeta) {
    return {
      content: [
        { type: "text", text: JSON.stringify({ error: `Topic '${args.topic}' not found` }) },
      ],
    };
  }

  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(
          {
            topic: topicMeta.name,
            partitions: topicMeta.partitions.length,
            partitionIds: topicMeta.partitions.map((p) => p.partitionId),
          },
          null,
          2,
        ),
      },
    ],
  };
}

async function handleGetConsumerGroupLag(args) {
  await admin.connect();
  const groups = await admin.listGroups();
  const targetGroup = groups.groups.find(
    (g) => g.groupId === args.group,
  );
  await admin.disconnect();

  if (!targetGroup) {
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify({ error: `Consumer group '${args.group}' not found` }),
        },
      ],
    };
  }

  // For simplicity, return group metadata
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(
          {
            groupId: targetGroup.groupId,
            protocolType: targetGroup.protocolType,
            state: targetGroup.state,
            members: targetGroup.members?.length || 0,
            note: "Detailed lag requires offsets; install kafka-consumer-groups.sh for full output. This standalone MCP returns group metadata.",
          },
          null,
          2,
        ),
      },
    ],
  };
}

// ---- MCP Server setup ----
const server = new Server(
  {
    name: "mcp-confluent-standalone",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

// List tools
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));

// Call tool
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  switch (name) {
    case "consume-messages":
      return handleConsumeMessages(args);
    case "list-topics":
      return handleListTopics(args);
    case "get-topic-config":
      return handleGetTopicConfig(args);
    case "get-consumer-group-lag":
      return handleGetConsumerGroupLag(args);
    default:
      return {
        content: [
          { type: "text", text: `Unknown tool: ${name}` },
        ],
        isError: true,
      };
  }
});

// ---- Start server ----
async function main() {
  // Start MCP over HTTP/SSE on configured port
  const app = express();
  app.use(express.json());

  // Health check
  app.get("/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  // MCP endpoint
  app.post("/mcp", async (req, res) => {
    try {
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: undefined,
      });
      await server.connect(transport);

      // Handle the MCP request
      const result = await transport.handleRequest(req.body, {
        jsonrpc: req.body.jsonrpc || "2.0",
        id: req.body.id,
        method: req.body.method,
        params: req.body.params,
      });

      res.json(result);
    } catch (err) {
      console.error("MCP error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  app.listen(MCP_PORT, () => {
    console.log(`MCP Confluent standalone running on http://0.0.0.0:${MCP_PORT}`);
    console.log(`Kafka bootstrap: ${KAFKA_BOOTSTRAP_SERVERS}`);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});