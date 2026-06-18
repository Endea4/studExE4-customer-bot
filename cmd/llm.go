package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type ExtractedRequest struct {
	GenderPref string `json:"gender_pref"`
	Notes      string `json:"notes"`
}

type llmMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type llmRequest struct {
	Model       string       `json:"model"`
	Messages    []llmMessage `json:"messages"`
	Temperature float64      `json:"temperature"`
}

type llmChoice struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
}

type llmResponse struct {
	Choices []llmChoice `json:"choices"`
}

func extractCustomRequest(text string) (*ExtractedRequest, error) {
	baseURL := os.Getenv("LLM_BASE_URL")
	apiKey := os.Getenv("LLM_API_KEY")
	model := os.Getenv("LLM_MODEL")

	if baseURL == "" || apiKey == "" || model == "" {
		return &ExtractedRequest{Notes: text}, nil
	}

	systemPrompt := `You are a ride-hailing assistant. Extract structured preferences from the customer's message.

Rules:
- gender_pref: "male" or "female" ONLY if explicitly mentioned, otherwise ""
- notes: everything else the customer mentioned, kept in original language (Indonesian). If no specific preferences beyond gender, set to "".
- If the message is unclear, just put it all in notes.

You MUST respond with ONLY valid JSON, no other text:
{"gender_pref": "male"|"female"|"", "notes": "..."}`

	reqBody := llmRequest{
		Model: model,
		Messages: []llmMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: fmt.Sprintf("Customer message: %s", text)},
		},
		Temperature: 0.1,
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", baseURL+"/chat/completions", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[LLM-ERROR] request failed: %v\n", err)
		return &ExtractedRequest{Notes: text}, nil
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	fmt.Printf("[LLM-RESP] status=%d body=%s\n", resp.StatusCode, string(respBody))

	var llmResp llmResponse
	if err := json.Unmarshal(respBody, &llmResp); err != nil || len(llmResp.Choices) == 0 {
		fmt.Printf("[LLM-ERROR] parse failed: %v\n", err)
		return &ExtractedRequest{Notes: text}, nil
	}

	content := llmResp.Choices[0].Message.Content
	fmt.Printf("[LLM-RAW] %s\n", content)

	var result ExtractedRequest
	if err := json.Unmarshal([]byte(content), &result); err != nil {
		lastBrace := bytes.LastIndex([]byte(content), []byte("{"))
		end := bytes.LastIndex([]byte(content), []byte("}"))
		if lastBrace >= 0 && end > lastBrace {
			json.Unmarshal([]byte(content[lastBrace:end+1]), &result)
		}
		if result.Notes == "" && result.GenderPref == "" {
			result.Notes = text
		}
	}

	if result.GenderPref != "male" && result.GenderPref != "female" {
		result.GenderPref = ""
	}

	fmt.Printf("[LLM-EXTRACTED] gender_pref=%q notes=%q\n", result.GenderPref, result.Notes)
	return &result, nil
}
