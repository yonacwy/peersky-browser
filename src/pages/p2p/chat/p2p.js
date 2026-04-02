/**
 * P2P chat over IPFS libp2p pubsub: room keys, message history, SSE, and peer messaging.
 * Uses IPFS gossipsub for real-time messaging instead of Hyperswarm.
 */
import { PassThrough } from "stream";
import b4a from "b4a";

// Room key -> array of messages (chat message history in memory)
const roomMessages = {};
// Room key -> array of SSE streams for real-time updates
const roomSseClients = {};

// Track connected peers per room
const roomPeerCounts = {};
const joinedRooms = new Set();

let pubsub = null;
let ipfsNode = null;
let chatInitPromise = null;

/** Room keys are 32-byte hex (64 chars). Reject invalid to avoid bad topic names. */
function isValidRoomKey(roomKey) {
  return typeof roomKey === "string" && /^[a-f0-9]{64}$/i.test(roomKey);
}

const MAX_SENDER_LENGTH = 200;
const MAX_MESSAGE_LENGTH = 64 * 1024; // 64KB

function sanitizeSendPayload(sender, message) {
  const s = typeof sender === "string" ? sender : String(sender ?? "");
  const m = typeof message === "string" ? message : String(message ?? "");
  if (s.length > MAX_SENDER_LENGTH) throw new Error("Sender too long");
  if (m.length > MAX_MESSAGE_LENGTH) throw new Error("Message too long");
  return { sender: s, message: m };
}

function getTopicName(roomKey) {
  return `/peersky/chat/${roomKey}`;
}

/** Initialize chat with retry logic to handle race conditions */
export async function initChat(node) {
  if (!node || !node.libp2p) {
    console.error("[Chat] Cannot initialize - no IPFS node provided");
    return;
  }
  
  // Prevent double initialization
  if (chatInitPromise) {
    return chatInitPromise;
  }
  
  chatInitPromise = (async () => {
    ipfsNode = node;
    
    // Wait for pubsub to be available (might not be immediately ready)
    let attempts = 0;
    const maxAttempts = 50; // 5 seconds total
    
    while (!pubsub && attempts < maxAttempts) {
      pubsub = node.libp2p.services.pubsub;
      if (!pubsub) {
        attempts++;
        console.log(`[Chat] Waiting for pubsub service... (${attempts}/${maxAttempts})`);
        await new Promise(r => setTimeout(r, 100));
      }
    }
    
    if (!pubsub) {
      console.error("[Chat] Pubsub service not available after retries");
      return;
    }
    
    console.log("[Chat] Initialized with IPFS pubsub");
    
    // Handle incoming pubsub messages
    pubsub.addEventListener('message', (event) => {
      const { topic, data, from } = event.detail;
      
      // Extract room key from topic name
      const topicMatch = topic.match(/^\/peersky\/chat\/([a-f0-9]{64})$/i);
      if (!topicMatch) return;
      
      const roomKey = topicMatch[1];
      const shortID = from ? from.toString().slice(0, 8) : "peer";
      
      try {
        const msg = JSON.parse(b4a.toString(data));
        
        // Only process messages from other peers (not self)
        if (msg.sender === ipfsNode.libp2p.peerId.toString()) {
          return;
        }
        
        console.log(`[Chat] Peer [${shortID}] => room ${roomKey.slice(0, 8)}...`, msg);
        
        // Store message in room history
        if (!roomMessages[roomKey]) {
          roomMessages[roomKey] = [];
        }
        
        const messageEntry = {
          sender: msg.sender || shortID,
          message: msg.message,
          timestamp: msg.timestamp || Date.now(),
        };
        
        roomMessages[roomKey].push(messageEntry);
        
        // Broadcast to SSE clients
        const sseArray = roomSseClients[roomKey] || [];
        for (const s of sseArray) {
          try {
            s.write(`data: ${JSON.stringify(messageEntry)}\n\n`);
          } catch (err) {
            console.error("[Chat] SSE write failed:", err);
          }
        }
      } catch (err) {
        console.error("[Chat] Failed to parse pubsub message:", err);
      }
    });
  })();
  
  return chatInitPromise;
}

/** Check if chat is ready */
export function isChatReady() {
  return !!pubsub;
}

function broadcastPeerCount(roomKey) {
  const count = roomPeerCounts[roomKey] || 0;
  const sseArray = roomSseClients[roomKey] || [];
  for (const s of sseArray) {
    try {
      s.write(`event: peersCount\ndata: ${count}\n\n`);
    } catch (err) {
      console.error("[Chat] SSE peer count write failed:", err);
    }
  }
}

async function subscribeToRoom(roomKey) {
  if (!pubsub) {
    throw new Error("Pubsub not initialized");
  }
  
  const topic = getTopicName(roomKey);
  
  try {
    await pubsub.subscribe(topic);
    console.log(`[Chat] Subscribed to topic: ${topic}`);
    
    // Get peer count for this topic
    const peers = pubsub.getPeers(topic);
    roomPeerCounts[roomKey] = peers.length;
    broadcastPeerCount(roomKey);
  } catch (err) {
    console.error(`[Chat] Failed to subscribe to ${topic}:`, err);
    throw err;
  }
}

async function publishToRoom(roomKey, data) {
  if (!pubsub) {
    throw new Error("Pubsub not initialized");
  }
  
  const topic = getTopicName(roomKey);
  const encodedData = b4a.from(JSON.stringify(data));
  
  try {
    await pubsub.publish(topic, encodedData);
    console.log(`[Chat] Published to ${roomKey.slice(0, 8)}... (${encodedData.length} bytes)`);
  } catch (err) {
    console.error(`[Chat] Failed to publish to ${topic}:`, err);
    throw err;
  }
}

function generateChatRoom() {
  const randomBuf = Buffer.alloc(32);
  globalThis.crypto.getRandomValues(randomBuf);
  return b4a.toString(randomBuf, "hex");
}

async function joinChatRoom(roomKey) {
  if (!isValidRoomKey(roomKey)) {
    throw new Error("Invalid roomKey format");
  }
  
  // Initialize message history for this room if needed
  if (!roomMessages[roomKey]) {
    roomMessages[roomKey] = [];
  }
  
  if (!joinedRooms.has(roomKey)) {
    joinedRooms.add(roomKey);
    await subscribeToRoom(roomKey);
    console.log(`[Chat] Joined room: ${roomKey}`);
  } else {
    console.log(`[Chat] Already joined room: ${roomKey}`);
  }
}

/**
 * Handle hyper://chat requests (create-key, join, send, receive/SSE).
 * Now uses IPFS pubsub under the hood instead of Hyperswarm.
 * @param {Request} req - Fetch-style protocol request
 */
export async function handleChatRequest(req) {
  const { url, method } = req;
  const urlObj = new URL(url);
  const action = urlObj.searchParams.get("action");
  const roomKey = urlObj.searchParams.get("roomKey");

  console.log(`[Chat] Request: ${method} ${url}`);
  console.log(`[Chat] Action: ${action}, RoomKey: ${roomKey}`);

  // Ensure pubsub is available
  if (!pubsub) {
    console.error("[Chat] Pubsub not available - returning 503");
    return new Response("Chat service not available - IPFS pubsub not initialized", {
      status: 503,
      headers: { "Content-Type": "text/plain" },
    });
  }

  console.log(`[Chat] Pubsub available, processing action: ${action}`);

  try {
    if (method === "POST" && action === "create-key") {
      const newRoomKey = generateChatRoom();
      console.log("[Chat] Generated new room key:", newRoomKey);
      return new Response(JSON.stringify({ roomKey: newRoomKey }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
      
    } else if (method === "POST" && action === "join") {
      if (!roomKey) throw new Error("Missing roomKey in join request");
      if (!isValidRoomKey(roomKey)) throw new Error("Invalid roomKey format");
      console.log("[Chat] Joining room:", roomKey);
      await joinChatRoom(roomKey);
      return new Response(JSON.stringify({ message: "Joined chat room" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
      
    } else if (method === "POST" && action === "send") {
      if (!roomKey) throw new Error("Missing roomKey in send request");
      if (!isValidRoomKey(roomKey)) throw new Error("Invalid roomKey format");
      const raw = await req.json();
      const { sender, message } = sanitizeSendPayload(raw.sender, raw.message);
      
      const peerId = ipfsNode?.libp2p?.peerId?.toString() || "unknown";
      const displaySender = sender || peerId.slice(0, 8);
      console.log(`[Chat] Sending message [${displaySender}]: ${message.substring(0, 50)}...`);

      const messageEntry = {
        sender: displaySender,
        message,
        timestamp: Date.now(),
      };

      // Store locally
      if (!roomMessages[roomKey]) {
        roomMessages[roomKey] = [];
      }
      roomMessages[roomKey].push(messageEntry);

      // Broadcast to pubsub
      await publishToRoom(roomKey, {
        sender: displaySender,
        message,
        timestamp: messageEntry.timestamp,
        roomKey,
      });

      // Broadcast to local SSE clients immediately
      const sseArray = roomSseClients[roomKey] || [];
      for (const s of sseArray) {
        s.write(`data: ${JSON.stringify(messageEntry)}\n\n`);
      }

      return new Response(JSON.stringify({ message: "Message sent" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
      
    } else if (method === "GET" && action === "receive") {
      if (!roomKey) throw new Error("Missing roomKey in receive request");
      if (!isValidRoomKey(roomKey)) throw new Error("Invalid roomKey format");
      console.log("[Chat] Setting up SSE for room:", roomKey);

      // Ensure room is joined
      if (!joinedRooms.has(roomKey)) {
        await joinChatRoom(roomKey);
      }

      const stream = new PassThrough();

      // Send existing message history
      const messages = roomMessages[roomKey] || [];
      for (const msg of messages) {
        stream.write(`data: ${JSON.stringify(msg)}\n\n`);
      }

      const keepAlive = setInterval(() => {
        stream.write(":\n\n");
      }, 15000);

      if (!roomSseClients[roomKey]) {
        roomSseClients[roomKey] = [];
      }
      roomSseClients[roomKey].push(stream);

      // Send initial peer count
      const count = roomPeerCounts[roomKey] || 0;
      stream.write(`event: peersCount\ndata: ${count}\n\n`);

      stream.on("close", () => {
        clearInterval(keepAlive);
        roomSseClients[roomKey] = roomSseClients[roomKey].filter(
          (s) => s !== stream
        );
      });

      return new Response(stream, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
        },
      });
      
    } else {
      return new Response("Invalid chat action", {
        status: 400,
        headers: { "Content-Type": "text/plain" },
      });
    }
  } catch (err) {
    console.error("[Chat] Error in handleChatRequest:", err);
    return new Response(`Error in chat request: ${err.message}`, {
      status: 500,
      headers: { "Content-Type": "text/plain" },
    });
  }
}
