#!/usr/bin/env bun
// this is used to spawn the minecraft server process

import { spawn } from "node:child_process";
import { parseArgs } from "node:util";
import type { Readable } from "node:stream";

const argv = process.argv.slice(2);
const sepIdx = argv.indexOf("--");
if (sepIdx === -1) {
  console.error("Usage: ./http-streams.ts -p <port> -- <command> [args...]");
  process.exit(1);
}

const ownArgs = argv.slice(0, sepIdx);
const cmdArgs = argv.slice(sepIdx + 1);

if (cmdArgs.length === 0) {
  console.error("Error: no command provided after --");
  process.exit(1);
}

const { values } = parseArgs({
  args: ownArgs,
  options: {
    port: { type: "string", short: "p" },
  },
  strict: true,
});

if (!values.port) {
  console.error("Error: -p <port> is required");
  process.exit(1);
}

const port = Number(values.port);
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  console.error(`Error: invalid port "${values.port}"`);
  process.exit(1);
}

const [cmd, ...args] = cmdArgs;

const child = spawn(cmd, args, {
  stdio: ["pipe", "pipe", "pipe"],
});

child.on("error", (err) => {
  console.error(`Failed to start command: ${err.message}`);
  process.exit(1);
});

process.stdin.pipe(child.stdin!, { end: false });
child.stdout!.pipe(process.stdout, { end: false });
child.stderr!.pipe(process.stderr, { end: false });

type LineBuffer = {
  lines: string[];
  ended: boolean;
  // Subscribers receive every new line as it arrives. They self-remove on
  // disconnect. Used by both long-poll (one-shot) and SSE (persistent).
  subscribers: Set<(line: string) => void>;
};

function makeBuffer(): LineBuffer {
  return { lines: [], ended: false, subscribers: new Set() };
}

function attachLineCapture(stream: Readable, buf: LineBuffer): void {
  let partial = "";
  stream.on("data", (chunk: Buffer) => {
    partial += chunk.toString("utf8");
    let nl: number;
    while ((nl = partial.indexOf("\n")) !== -1) {
      const line = partial.slice(0, nl);
      partial = partial.slice(nl + 1);
      buf.lines.push(line);
      for (const sub of buf.subscribers) sub(line);
    }
  });
  stream.on("end", () => {
    if (partial.length > 0) {
      buf.lines.push(partial);
      for (const sub of buf.subscribers) sub(partial);
      partial = "";
    }
    buf.ended = true;
    // Wake any subscribers that need to know the stream closed.
    for (const sub of buf.subscribers) sub("");
  });
}

const stdoutBuf = makeBuffer();
const stderrBuf = makeBuffer();
attachLineCapture(child.stdout!, stdoutBuf);
attachLineCapture(child.stderr!, stderrBuf);

child.on("exit", (code, signal) => {
  server.stop(true);
  if (signal) process.kill(process.pid, signal);
  else process.exit(code ?? 0);
});

for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"] as const) {
  process.on(sig, () => child.kill(sig));
}

async function handlePoll(req: Request, buf: LineBuffer): Promise<Response> {
  const url = new URL(req.url);
  const sinceParam = url.searchParams.get("since");
  const since = sinceParam ? Math.max(0, parseInt(sinceParam, 10) || 0) : 0;

  if (since < buf.lines.length) {
    return Response.json({
      lines: buf.lines.slice(since),
      next: buf.lines.length,
      ended: buf.ended,
    });
  }

  if (buf.ended) {
    return Response.json({ lines: [], next: buf.lines.length, ended: true });
  }

  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      buf.subscribers.delete(sub);
      resolve();
    }, 30_000);
    const sub = () => {
      clearTimeout(timer);
      buf.subscribers.delete(sub);
      resolve();
    };
    buf.subscribers.add(sub);
  });

  return Response.json({
    lines: buf.lines.slice(since),
    next: buf.lines.length,
    ended: buf.ended,
  });
}

function handleStream(req: Request, buf: LineBuffer): Response {
  const url = new URL(req.url);
  const sinceParam = url.searchParams.get("since");
  const since = sinceParam ? Math.max(0, parseInt(sinceParam, 10) || 0) : 0;

  const encoder = new TextEncoder();
  let cursor = since;
  let sub: ((line: string) => void) | null = null;
  let heartbeat: ReturnType<typeof setInterval> | null = null;

  const body = new ReadableStream({
    start(controller) {
      const sendLine = (idx: number, line: string) => {
        // SSE: id lets client reconnect via Last-Event-ID (or ?since=).
        const payload = `id: ${idx}\ndata: ${JSON.stringify(line)}\n\n`;
        controller.enqueue(encoder.encode(payload));
      };

      for (; cursor < buf.lines.length; cursor++) {
        sendLine(cursor, buf.lines[cursor]);
      }

      if (buf.ended) {
        controller.enqueue(encoder.encode("event: end\ndata: {}\n\n"));
        controller.close();
        return;
      }

      sub = () => {
        while (cursor < buf.lines.length) {
          sendLine(cursor, buf.lines[cursor]);
          cursor++;
        }
        if (buf.ended) {
          controller.enqueue(encoder.encode("event: end\ndata: {}\n\n"));
          if (heartbeat) clearInterval(heartbeat);
          if (sub) buf.subscribers.delete(sub);
          controller.close();
        }
      };
      buf.subscribers.add(sub);

      // Keep proxies from closing the connection on idle.
      heartbeat = setInterval(() => {
        controller.enqueue(encoder.encode(": ping\n\n"));
      }, 15_000);

      req.signal.addEventListener("abort", () => {
        if (sub) buf.subscribers.delete(sub);
        if (heartbeat) clearInterval(heartbeat);
        try {
          controller.close();
        } catch {}
      });
    },
    cancel() {
      if (sub) buf.subscribers.delete(sub);
      if (heartbeat) clearInterval(heartbeat);
    },
  });

  return new Response(body, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}

const server = Bun.serve({
  port,
  async fetch(req) {
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/stdin") {
      if (!child.stdin || child.stdin.destroyed) {
        return new Response("stdin closed", { status: 410 });
      }
      const body = await req.text();
      const payload = body.endsWith("\n") ? body : body + "\n";
      const ok = child.stdin.write(payload);
      if (!ok) await new Promise<void>((r) => child.stdin!.once("drain", r));
      return new Response("ok\n");
    }

    if (req.method === "GET" && url.pathname === "/stdout") {
      return handlePoll(req, stdoutBuf);
    }
    if (req.method === "GET" && url.pathname === "/stderr") {
      return handlePoll(req, stderrBuf);
    }

    if (req.method === "GET" && url.pathname === "/stdout/stream") {
      return handleStream(req, stdoutBuf);
    }
    if (req.method === "GET" && url.pathname === "/stderr/stream") {
      return handleStream(req, stderrBuf);
    }

    return new Response("not found\n", { status: 404 });
  },
});

console.error(`http-streams listening on http://localhost:${server.port}`);
