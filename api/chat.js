export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      error: "Method not allowed"
    });
  }

  try {
    const { messages } = req.body || {};

    if (!Array.isArray(messages) || messages.length === 0) {
      return res.status(400).json({
        error: "Messages are required"
      });
    }

    // Keep only recent messages to reduce request size and latency
    const recentMessages = messages.slice(-12);

    const contents = recentMessages.map((msg) => ({
      role: msg.role === "assistant" ? "model" : "user",
      parts: [
        {
          text: String(msg.content || "")
        }
      ]
    }));

    const response = await fetch(
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
      {
        method: "POST",

        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": process.env.GEMINI_API_KEY,
          "Accept": "text/event-stream"
        },

        body: JSON.stringify({
          contents,

          generationConfig: {
            temperature: 0.4,
            maxOutputTokens: 2048
          }
        })
      }
    );

    if (!response.ok) {
      const errorText = await response.text();

      console.error("Gemini API error:", errorText);

      let errorMessage = "Gemini API request failed.";

      try {
        const errorData = JSON.parse(errorText);

        errorMessage =
          errorData?.error?.message ||
          errorMessage;

      } catch {
        // Response wasn't JSON
      }

      return res.status(response.status).json({
        error: errorMessage
      });
    }

    if (!response.body) {
      return res.status(500).json({
        error: "Gemini did not return a stream."
      });
    }

    // SSE headers
    res.setHeader(
      "Content-Type",
      "text/event-stream; charset=utf-8"
    );

    res.setHeader(
      "Cache-Control",
      "no-cache, no-transform"
    );

    res.setHeader(
      "Connection",
      "keep-alive"
    );

    // Helpful for some hosting/proxy environments
    res.setHeader(
      "X-Accel-Buffering",
      "no"
    );

    const reader = response.body.getReader();

    const decoder = new TextDecoder();

    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();

      if (done) {
        break;
      }

      buffer += decoder.decode(value, {
        stream: true
      });

      const events = buffer.split("\n\n");

      buffer = events.pop() || "";

      for (const event of events) {

        const lines = event.split("\n");

        for (const line of lines) {

          if (!line.startsWith("data:")) {
            continue;
          }

          const jsonText =
            line.slice(5).trim();

          if (!jsonText) {
            continue;
          }

          try {

            const data =
              JSON.parse(jsonText);

            const parts =
              data?.candidates?.[0]
                ?.content?.parts || [];

            for (const part of parts) {

              if (part.text) {

                res.write(
                  `data: ${JSON.stringify({
                    text: part.text
                  })}\n\n`
                );

              }

            }

          } catch (error) {

            console.error(
              "Stream parsing error:",
              error
            );

          }
        }
      }
    }

    res.write(
      `data: ${JSON.stringify({
        done: true
      })}\n\n`
    );

    res.end();

  } catch (error) {

    console.error(
      "Server error:",
      error
    );

    if (!res.headersSent) {

      return res.status(500).json({
        error:
          error?.message ||
          "Something went wrong."
      });

    }

    res.write(
      `data: ${JSON.stringify({
        error:
          error?.message ||
          "Something went wrong."
      })}\n\n`
    );

    res.end();
  }
}
