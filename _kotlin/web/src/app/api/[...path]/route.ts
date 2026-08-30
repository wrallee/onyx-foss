import { NextRequest, NextResponse } from "next/server";

const API_BASE_URL =
  process.env.API_BASE_URL || process.env.INTERNAL_URL || "http://api:8080";

export async function GET(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

export async function POST(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

export async function PUT(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

export async function PATCH(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

export async function DELETE(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

export async function HEAD(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

export async function OPTIONS(
  request: NextRequest,
  props: { params: Promise<{ path: string[] }> }
) {
  return handleRequest(request, (await props.params).path);
}

async function handleRequest(request: NextRequest, path: string[]) {
  try {
    const backendUrl = new URL(
      `${API_BASE_URL.replace(/\/$/, "")}/${path.join("/")}`
    );
    const requestUrl = new URL(request.url);
    requestUrl.searchParams.forEach((value, key) => {
      backendUrl.searchParams.append(key, value);
    });

    const headers = new Headers(request.headers);
    headers.delete("host");
    const response = await fetch(backendUrl, {
      method: request.method,
      headers,
      body: request.body,
      signal: request.signal,
      redirect: "manual",
      // Node requires duplex for streamed request bodies.
      // @ts-expect-error undici extension
      duplex: "half",
    });

    const responseHeaders = new Headers(response.headers);
    responseHeaders.delete("set-cookie");
    if (
      response.headers.get("Transfer-Encoding") === "chunked" ||
      response.headers.get("Content-Type")?.includes("stream")
    ) {
      responseHeaders.set("Cache-Control", "no-cache, no-transform");
      responseHeaders.set("X-Accel-Buffering", "no");
      responseHeaders.delete("content-length");
    }

    return new NextResponse(response.body, {
      status: response.status,
      headers: responseHeaders,
    });
  } catch (error: unknown) {
    console.error("Kotlin API proxy error:", error);
    return NextResponse.json(
      {
        message: "API proxy error",
        error: error instanceof Error ? error.message : "An unknown error occurred",
      },
      { status: 502 }
    );
  }
}
