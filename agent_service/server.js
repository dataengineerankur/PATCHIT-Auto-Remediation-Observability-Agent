import http from "node:http";
import { URL } from "node:url";

import Bytez from "bytez.js";

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => {
      try {
        resolve(JSON.parse(data || "{}"));
      } catch (e) {
        reject(e);
      }
    });
  });
}

function send(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { "content-type": "application/json" });
  res.end(body);
}

function buildPrompt(payload) {
  const { event, normalized_error, code_context, artifact_summaries, codebase_index } = payload;

  return [
    "You are ARDOA Fix Agent. Generate a minimal SAFE patch as a unified diff.",
    "",
    "HARD RULES:",
    "- Output ONLY a unified diff (git apply compatible). No prose outside the diff.",
    "- Touch the fewest files possible. Prefer targeted guardrails (validation, try/except, retries).",
    "- Do NOT modify: .git/, venv/, .venv/, terraform/, secrets/, config.env",
    "- Assume human review; do not introduce risky broad refactors.",
    "",
    "FAILURE EVENT (JSON):",
    JSON.stringify(event, null, 2),
    "",
    "NORMALIZED ERROR (JSON):",
    JSON.stringify(normalized_error, null, 2),
    "",
    "ARTIFACT SUMMARIES (JSON):",
    JSON.stringify(artifact_summaries || [], null, 2),
    "",
    "CODE CONTEXT SNIPPETS (JSON):",
    JSON.stringify(code_context || {}, null, 2),
    "",
    "CODEBASE INDEX (JSON):",
    JSON.stringify(codebase_index || {}, null, 2),
    "",
    "TASK:",
    "- Propose a patch to fix the likely root cause.",
    "- If the failure is in a DAG/task code file, patch that file. If it is in a shared lib, patch shared lib.",
    "- Ensure the patch compiles and won’t break other scenarios.",
    "",
    "OUTPUT:",
    "- Unified diff only."
  ].join("\n");
}

const PORT = parseInt(process.env.AGENT_PORT || "8098", 10);
const BYTEZ_API_KEY = process.env.BYTEZ_API_KEY;
const MODEL_NAME = process.env.BYTEZ_MODEL || "anthropic/claude-sonnet-4-5";

if (!BYTEZ_API_KEY) {
  console.error("BYTEZ_API_KEY is required");
}

const sdk = BYTEZ_API_KEY ? new Bytez(BYTEZ_API_KEY) : null;
const model = sdk ? sdk.model(MODEL_NAME) : null;

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (req.method === "GET" && url.pathname === "/health") {
    return send(res, 200, { ok: true, model: MODEL_NAME, has_key: !!BYTEZ_API_KEY });
  }

  if (req.method === "POST" && url.pathname === "/v1/propose_patch") {
    if (!model) return send(res, 400, { error: "BYTEZ_API_KEY not configured" });
    try {
      const payload = await readJson(req);
      const prompt = buildPrompt(payload);
      const { error, output } = await model.run([
        { role: "user", content: prompt }
      ]);
      if (error) return send(res, 502, { error: String(error) });
      return send(res, 200, { diff: String(output || "") });
    } catch (e) {
      return send(res, 500, { error: String(e) });
    }
  }

  return send(res, 404, { error: "not_found" });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`ardoa-agent listening on :${PORT}, model=${MODEL_NAME}`);
});


