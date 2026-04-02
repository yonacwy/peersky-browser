import { Readable } from "stream";
import { create as createSDK } from "hyper-sdk";
import makeHyperFetch from "hypercore-fetch";
import { initChat, handleChatRequest as handleChatRequestP2P } from "../pages/p2p/chat/p2p.js";
import { hyperCache, saveHyperCache } from "./config.js";

// Single SDK and swarm for the app lifecycle (hyper:// browsing + chat share the same swarm).
let sdk, fetch;
let sdkInitializing = false;
let sdkInitError = null;

// keep chunks smaller to avoid oversized blocks.
const MAX_UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024;

function isWebReadableStream(body) {
  return body && typeof body.getReader === "function";
}

function isAsyncIterable(body) {
  return body && typeof body[Symbol.asyncIterator] === "function";
}

async function* readWebStream(stream) {
  const reader = stream.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) yield value;
    }
  } finally {
    if (reader.releaseLock) reader.releaseLock();
  }
}

async function* chunkAsyncIterable(iterable, chunkSize) {
  for await (const chunk of iterable) {
    if (chunk == null) continue;
    const buf = Buffer.isBuffer(chunk)
      ? chunk
      : chunk instanceof Uint8Array
        ? Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
        : Buffer.from(chunk);
    for (let offset = 0; offset < buf.length; offset += chunkSize) {
      yield buf.subarray(offset, offset + chunkSize);
    }
  }
}

function getChunkedBody(req) {
  const body = req.body;
  if (!body) return body;

  const contentType = req.headers?.get?.("content-type") || "";
  if (contentType.includes("multipart/form-data")) {
    return body;
  }

  const iterable = isWebReadableStream(body)
    ? readWebStream(body)
    : isAsyncIterable(body)
      ? body
      : null;

  if (!iterable) {
    if (Buffer.isBuffer(body)) {
      return Readable.from(chunkAsyncIterable([body], MAX_UPLOAD_CHUNK_BYTES));
    }
    if (body instanceof Uint8Array) {
      const buf = Buffer.from(body.buffer, body.byteOffset, body.byteLength);
      return Readable.from(chunkAsyncIterable([buf], MAX_UPLOAD_CHUNK_BYTES));
    }
    if (body instanceof ArrayBuffer) {
      return Readable.from(
        chunkAsyncIterable([Buffer.from(body)], MAX_UPLOAD_CHUNK_BYTES)
      );
    }
    if (typeof body === "string") {
      return Readable.from(
        chunkAsyncIterable([Buffer.from(body)], MAX_UPLOAD_CHUNK_BYTES)
      );
    }
    return body;
  }
  return Readable.from(chunkAsyncIterable(iterable, MAX_UPLOAD_CHUNK_BYTES));
}

async function initializeHyperSDK(options) {
  if (sdk != null && fetch != null) return fetch;
  if (sdkInitError) throw sdkInitError;
  if (sdkInitializing) {
    // Wait for initialization to complete
    while (sdkInitializing) {
      await new Promise(r => setTimeout(r, 100));
    }
    if (sdkInitError) throw sdkInitError;
    return fetch;
  }

  sdkInitializing = true;
  console.log("Initializing Hyper SDK...");

  try {
    sdk = await createSDK(options);
    fetch = makeHyperFetch({ sdk, writable: true });
    initChat(sdk);
    console.log("Hyper SDK initialized.");
  } catch (err) {
    console.error("Hyper SDK initialization failed:", err);
    sdkInitError = err;
    throw err;
  } finally {
    sdkInitializing = false;
  }
  return fetch;
}

export async function createHandler(options) {
  // Defer SDK initialization until first request to avoid startup crashes
  const hyperOptions = options;

  return async function protocolHandler(req) {
    const { url, method } = req;
    const urlObj = new URL(url);
    const protocol = urlObj.protocol.replace(":", "");
    const pathname = urlObj.pathname;

    console.log(`Handling request: ${method} ${url}`);

    // Handle chat requests first (uses IPFS pubsub, not Hyper SDK)
    if (
      protocol === "hyper" &&
      (urlObj.hostname === "chat" || pathname.startsWith("/chat"))
    ) {
      console.log(`[Hyper] Routing to IPFS chat: ${method} ${url}`);
      // IPFS-based chat (Hyper SDK disabled, uses pubsub instead)
      return await handleChatRequestP2P(req);
    }

    // Block Hyper SDK for IPFS content (bafy addresses) to prevent crashes
    if (
      protocol === "hyper" &&
      (urlObj.hostname.startsWith("bafy") || urlObj.hostname.startsWith("ba"))
    ) {
      console.log(`[Hyper] Blocking IPFS content URL: ${method} ${url}`);
      return new Response("IPFS content should use ipfs:// protocol", {
        status: 400,
        headers: { "Content-Type": "text/plain" }
      });
    }

    // All other hyper:// requests require Hyper SDK
    try {
      await initializeHyperSDK(hyperOptions);
    } catch (err) {
      console.error("[Hyper] SDK unavailable, returning 503");
      return new Response("Hyper SDK failed to initialize: " + err.message, {
        status: 503,
        headers: { "Content-Type": "text/plain" }
      });
    }

    // Intercept Hyperdrive key generation/retrieval
    if (method === 'POST' && urlObj.searchParams.has('key')) {
      const keyName = urlObj.searchParams.get('key');
      try {
        const fetchFn = await initializeHyperSDK();
        const resp = await fetchFn(url, {
          method,
          headers: req.headers,
          body: getChunkedBody(req),
          duplex: "half",
        });
        if (resp.status === 200) {
          const buffer = await resp.arrayBuffer();
          const driveKeyStr = Buffer.from(buffer).toString();
          console.log("Extracted raw key response:", driveKeyStr);

          const match = driveKeyStr.match(/([0-9a-zA-Z]{52,64})/);
          if (match) {
            const driveKey = match[1];
            const timestamp = Date.now();
            const existingEntry = hyperCache.find(entry => entry.key === driveKey);
            if (!existingEntry) {
              hyperCache.push({
                name: keyName || "Drive",
                key: driveKey,
                timestamp: timestamp,
                type: 'drive'
              });
              saveHyperCache();
              console.log(`Logged Hyperdrive to cache: ${keyName} (${driveKey})`);
            } else {
              existingEntry.timestamp = timestamp;
              if (keyName && (existingEntry.name === "Drive" || !existingEntry.name)) {
                existingEntry.name = keyName;
              }
              saveHyperCache();
              console.log(`Updated Hyperdrive in cache: ${keyName} (${driveKey})`);
            }
          }
          return new Response(buffer, {
            status: resp.status,
            headers: Object.fromEntries(resp.headers),
          });
        }
        return resp;
      } catch (err) {
        console.error("Error handling Hyperdrive key request:", err);
        return new Response(`Error handling Hyperdrive key request: ${err.message}`, {
          status: 500,
          headers: { "Content-Type": "text/plain" },
        });
      }
    }

    try {
      return await handleHyperRequest(req);
    } catch (err) {
      console.error("Failed to handle Hyper request:", err);
      return new Response(`Error handling Hyper request: ${err.message}`, {
        status: 500,
        headers: { "Content-Type": "text/plain" },
      });
    }
  };
}

// Handle general hyper:// requests (not chat API).
async function handleHyperRequest(req) {
  const { url, method = "GET", headers } = req;
  const fetchFn = await initializeHyperSDK();

  const upperMethod = method.toUpperCase();
  const hasBody = upperMethod !== "GET" && upperMethod !== "HEAD";

  try {
    console.log(`[handleHyperRequest] Fetching: ${method} ${url}`);
    const resp = await fetchFn(url, {
      method,
      headers,
      body: hasBody ? getChunkedBody(req) : undefined,
      ...(hasBody ? { duplex: "half" } : {}),
    });

    console.log("Response received:", resp.status);
    return resp;
  } catch (err) {
    console.error("Failed to fetch from Hyper SDK:", err);
    return new Response(`Error fetching data: ${err.message}`, {
      status: 500,
      headers: { "Content-Type": "text/plain" },
    });
  }
}
