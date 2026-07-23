import assert from "node:assert/strict";
import test from "node:test";

import worker from "./index.js";

class FakeObject {
  constructor(body, contentType = "text/plain; charset=utf-8") {
    this.body = body;
    this.contentType = contentType;
    this.httpEtag = '"test-etag"';
  }

  async text() {
    return this.body;
  }

  writeHttpMetadata(headers) {
    headers.set("content-type", this.contentType);
  }
}

class FakeBucket {
  constructor(objects) {
    this.objects = objects;
  }

  async get(key) {
    return this.objects.get(key) ?? null;
  }
}

test("root llms.txt redirects through the published snapshot alias", async () => {
  const bucket = new FakeBucket(
    new Map([["meta/snapshot.txt", new FakeObject("v1.2.3\n")]]),
  );

  const response = await worker.fetch(
    new Request("https://docs.nervix.io/llms.txt"),
    { DOCS_BUCKET: bucket },
  );

  assert.equal(response.status, 307);
  assert.equal(
    response.headers.get("location"),
    "https://docs.nervix.io/v1.2.3/llms.txt",
  );
  assert.equal(response.headers.get("cache-control"), "public, max-age=300");
});

test("versioned Markdown is served without HTML fallback", async () => {
  const bucket = new FakeBucket(
    new Map([
      [
        "v1.2.3/markdown/nspl-overview.md",
        new FakeObject("# NSPL Overview\n", "text/markdown; charset=utf-8"),
      ],
    ]),
  );

  const response = await worker.fetch(
    new Request(
      "https://docs.nervix.io/v1.2.3/markdown/nspl-overview.md",
    ),
    { DOCS_BUCKET: bucket },
  );

  assert.equal(response.status, 200);
  assert.equal(
    response.headers.get("content-type"),
    "text/markdown; charset=utf-8",
  );
  assert.equal(await response.text(), "# NSPL Overview\n");
});
