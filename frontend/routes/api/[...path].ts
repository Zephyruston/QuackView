import { Handlers } from "$fresh/server.ts";

const API_URL = "http://localhost:8000";

export const handler: Handlers = {
  async GET(req, ctx) {
    const url = new URL(req.url);
    const path = ctx.params.path;
    const apiUrl = `${API_URL}/api/${path}${url.search}`;

    console.log(`[API Proxy] GET ${path} -> ${apiUrl}`);

    try {
      const response = await fetch(apiUrl, {
        headers: req.headers,
      });

      const headers = new Headers();
      for (const [key, value] of response.headers.entries()) {
        headers.set(key, value);
      }

      if (
        response.headers
          .get("content-type")
          ?.includes("application/octet-stream") ||
        response.headers
          .get("content-type")
          ?.includes("application/vnd.openxmlformats")
      ) {
        const blob = await response.blob();
        return new Response(blob, {
          status: response.status,
          headers,
        });
      }

      const data = await response.json();
      return Response.json(data, {
        status: response.status,
        headers,
      });
    } catch (error) {
      console.error(`API请求错误 (${path}):`, error);
      return Response.json(
        { detail: "API请求失败，请检查后端服务是否运行" },
        { status: 500 },
      );
    }
  },

  async POST(req, ctx) {
    const path = ctx.params.path;
    const apiUrl = `${API_URL}/api/${path}`;

    console.log(`[API Proxy] POST ${path} -> ${apiUrl}`);

    try {
      if (path === "connection") {
        console.log(`[API Proxy] 处理文件上传请求`);
        const formData = await req.formData();
        console.log(`[API Proxy] FormData 大小: ${formData.entries()} 项`);

        const response = await fetch(apiUrl, {
          method: "POST",
          body: formData,
        });

        console.log(`[API Proxy] 后端响应状态: ${response.status}`);

        if (!response.ok) {
          const errorText = await response.text();
          console.error(`后端API错误 (${path}):`, errorText);
          return Response.json(
            {
              detail: `后端API错误: ${response.status} ${response.statusText}`,
            },
            { status: response.status },
          );
        }

        const contentType = response.headers.get("content-type");
        console.log(`[API Proxy] 响应Content-Type: ${contentType}`);

        if (contentType && contentType.includes("application/json")) {
          const data = await response.json();
          console.log(`[API Proxy] 返回JSON数据:`, data);
          return Response.json(data, {
            status: response.status,
          });
        } else {
          const text = await response.text();
          console.log(`[API Proxy] 返回文本响应:`, text);
          return new Response(text, {
            status: response.status,
            headers: {
              "Content-Type": "text/plain",
            },
          });
        }
      }

      const body = await req.json();
      const response = await fetch(apiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`后端API错误 (${path}):`, errorText);
        return Response.json(
          { detail: `后端API错误: ${response.status} ${response.statusText}` },
          { status: response.status },
        );
      }

      const data = await response.json();
      return Response.json(data, {
        status: response.status,
      });
    } catch (error) {
      console.error(`API请求错误 (${path}):`, error);
      return Response.json(
        { detail: "API请求失败，请检查后端服务是否运行" },
        { status: 500 },
      );
    }
  },

  async DELETE(req, ctx) {
    const url = new URL(req.url);
    const path = ctx.params.path;
    const apiUrl = `${API_URL}/api/${path}${url.search}`;

    console.log(`[API Proxy] DELETE ${path} -> ${apiUrl}`);

    try {
      const response = await fetch(apiUrl, {
        method: "DELETE",
        headers: req.headers,
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`后端API错误 (${path}):`, errorText);
        return Response.json(
          { detail: `后端API错误: ${response.status} ${response.statusText}` },
          { status: response.status },
        );
      }

      const data = await response.json();
      return Response.json(data, {
        status: response.status,
      });
    } catch (error) {
      console.error(`API请求错误 (${path}):`, error);
      return Response.json(
        { detail: "API请求失败，请检查后端服务是否运行" },
        { status: 500 },
      );
    }
  },
};
