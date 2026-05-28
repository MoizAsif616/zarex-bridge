package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	qrterminal "github.com/mdp/qrterminal/v3"
)

var client *whatsmeow.Client
var allowedNumber string
var pairedTime time.Time

type MessagePayload struct {
	To   string `json:"to"`
	Text string `json:"text"`
}

type PresencePayload struct {
	To    string `json:"to"`
	State string `json:"state"`
}

func getMasterNumber() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	configPath := filepath.Join(home, ".zarex", "config.toml")
	file, err := os.Open(configPath)
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "MASTER_NUMBER") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				val := strings.TrimSpace(parts[1])
				val = strings.Trim(val, `"'`)
				return val
			}
		}
	}
	return ""
}

func eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.LoggedOut:
		fmt.Println("[CRITICAL] Logged out from WhatsApp. Session invalidated.")
		os.Exit(1)
	case *events.Message:
		if v.Info.IsFromMe {
			return
		}
		if v.Info.Chat.String() == "status@broadcast" {
			return
		}
		// Get real phone number — WhatsApp hides it behind LID
		realSender := v.Info.Sender.User
		if v.Info.SenderAlt.User != "" {
			realSender = v.Info.SenderAlt.User
		}

		// Check for audio/voice message
		audioMsg := v.Message.GetAudioMessage()
		if audioMsg != nil {
			fmt.Printf("[MSG] Sender: %s | RealNumber: %s | Audio Message detected\n",
				v.Info.Sender.String(),
				realSender,
			)

			if allowedNumber != "" && realSender != allowedNumber {
				return
			}

			// Download and decrypt the audio media natively
			data, err := client.Download(context.Background(), audioMsg)
			if err != nil {
				fmt.Println("[ERROR] Failed to download audio message:", err)
				return
			}

			// Forward raw audio bytes to Python server
			go func(sender string, audioBytes []byte) {
				req, err := http.NewRequest("POST", "http://127.0.0.1:45050/whatsapp/incoming/audio", bytes.NewReader(audioBytes))
				if err != nil {
					fmt.Println("[ERROR] Failed to create HTTP request:", err)
					return
				}
				req.Header.Set("Content-Type", "application/octet-stream")
				req.Header.Set("X-Sender", sender)

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					fmt.Println("[ERROR] Failed to forward audio to Python:", err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != 200 {
					fmt.Println("[WARN] Python server returned status for audio:", resp.StatusCode)
				} else {
					fmt.Println("[+] Forwarded audio to Python server successfully")
					// Mark message as read ONLY on successful 200 OK delivery (ticks turn blue)
					err = client.MarkRead(
						context.Background(),
						[]types.MessageID{v.Info.ID},
						time.Now(),
						v.Info.Chat,
						v.Info.Sender,
					)
					if err != nil {
						fmt.Println("[WARN] Could not mark audio as read:", err)
					}
				}
			}(realSender, data)
			return
		}

		// Text Message processing
		msg := v.Message.GetConversation()
		if msg == "" {
			msg = v.Message.GetExtendedTextMessage().GetText()
		}
		if msg == "" {
			return
		}

		fmt.Printf("[MSG] Sender: %s | RealNumber: %s | Text: %s\n",
			v.Info.Sender.String(),
			realSender,
			msg,
		)

		if allowedNumber != "" && realSender != allowedNumber {
			return
		}

		// Forward text to Python server
		go func(sender, text string) {
			payload := map[string]string{
				"sender":  sender,
				"message": text,
			}
			jsonData, err := json.Marshal(payload)
			if err != nil {
				fmt.Println("[ERROR] Failed to marshal JSON:", err)
				return
			}
			resp, err := http.Post("http://127.0.0.1:45050/whatsapp/incoming/text", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				fmt.Println("[ERROR] Failed to forward text to Python:", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				fmt.Println("[WARN] Python server returned status for text:", resp.StatusCode)
			} else {
				fmt.Println("[+] Forwarded text message to Python server successfully")
				// Mark message as read ONLY on successful 200 OK delivery (ticks turn blue)
				err = client.MarkRead(
					context.Background(),
					[]types.MessageID{v.Info.ID},
					time.Now(),
					v.Info.Chat,
					v.Info.Sender,
				)
				if err != nil {
					fmt.Println("[WARN] Could not mark text as read:", err)
				}
			}
		}(realSender, msg)
	}
}

func sendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var payload MessagePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	jid, ok := parseJID(payload.To)
	if !ok {
		http.Error(w, "invalid JID format", http.StatusBadRequest)
		return
	}

	// Auto-stop typing indicator when sending the response message
	_ = client.SendChatPresence(context.Background(), jid, types.ChatPresencePaused, types.ChatPresenceMediaText)

	// Send message with a 5-second context timeout to prevent hanging the HTTP handler
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.SendMessage(ctx, jid, &waProto.Message{
		Conversation: proto.String(payload.Text),
	})
	if err != nil {
		fmt.Println("[ERROR] Failed to send message via whatsmeow:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"sent"}`))
}

func presenceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var payload PresencePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	jid, ok := parseJID(payload.To)
	if !ok {
		http.Error(w, "invalid JID format", http.StatusBadRequest)
		return
	}

	state := types.ChatPresencePaused
	if payload.State == "composing" {
		state = types.ChatPresenceComposing
	}

	err := client.SendChatPresence(context.Background(), jid, state, types.ChatPresenceMediaText)
	if err != nil {
		fmt.Println("[WARN] Failed to send chat presence:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"success"}`))
}

func parseJID(s string) (types.JID, bool) {
	jid, err := types.ParseJID(s)
	if err != nil {
		return types.JID{}, false
	}
	return jid, true
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if client != nil && client.IsConnected() && client.IsLoggedIn() {
		// Enforce a 10-second grace period immediately after pairing to let WhatsApp sessions initialize
		if !pairedTime.IsZero() && time.Since(pairedTime) < 10*time.Second {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"initializing_grace_period"}`))
			return
		}
		w.Write([]byte(`{"status":"ready"}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"not_ready"}`))
	}
}

func main() {
	allowedNumber = getMasterNumber()
	if allowedNumber == "" {
		fmt.Println("[CRITICAL] Master Phone Number is not set or empty in config.toml! This is mandatory. Exiting...")
		os.Exit(1)
	}

	// Instant crash-protection: exit immediately if parent process (Python) closes the stdin pipe
	go func() {
		_, err := os.Stdin.Read(make([]byte, 1))
		if err != nil {
			fmt.Println("[CRITICAL] Parent process connection lost. Exiting Go bridge...")
			os.Exit(0)
		}
	}()

	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	zarexDir := filepath.Join(home, ".zarex")
	if err := os.MkdirAll(zarexDir, 0700); err != nil {
		panic(err)
	}
	dbPath := filepath.Join(zarexDir, "session.db")

	logger := waLog.Stdout("Main", "ERROR", true)

	container, err := sqlstore.New(context.Background(), "sqlite", "file:"+dbPath+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", waLog.Stdout("DB", "ERROR", true))
	if err != nil {
		panic(err)
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		panic(err)
	}

	client = whatsmeow.NewClient(deviceStore, logger)
	client.ManualHistorySyncDownload = true // skip auto history sync
	client.AddEventHandler(eventHandler)

	if client.Store.ID == nil {
		qrChan, _ := client.GetQRChannel(context.Background())
		if err := client.Connect(); err != nil {
			panic(err)
		}
		for evt := range qrChan {
			if evt.Event == "code" {
				qrterminal.GenerateWithConfig(evt.Code, qrterminal.Config{
					Level:      qrterminal.L,
					Writer:     os.Stdout,
					HalfBlocks: true,
					QuietZone:  1,
				})
				fmt.Println("[QR_IMAGE_READY]")
			}
		}
		pairedTime = time.Now()
	} else {
		if err := client.Connect(); err != nil {
			panic(err)
		}
	}

	http.HandleFunc("/send", sendHandler)
	http.HandleFunc("/presence", presenceHandler)
	http.HandleFunc("/health", healthHandler)
	fmt.Println("[+] HTTP bridge running on 127.0.0.1:45051")
	if err := http.ListenAndServe("127.0.0.1:45051", nil); err != nil {
		panic(err)
	}
}
