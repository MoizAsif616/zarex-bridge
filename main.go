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

	qrcode "github.com/skip2/go-qrcode"
)

var client *whatsmeow.Client
var allowedNumber string

type MessagePayload struct {
	To   string `json:"to"`
	Text string `json:"text"`
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

		err := client.MarkRead(
			context.Background(),
			[]types.MessageID{v.Info.ID},
			time.Now(),
			v.Info.Chat,
			v.Info.Sender,
		)
		if err != nil {
			fmt.Println("[WARN] Could not mark as read:", err)
		}

		// Forward to Python server
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
			resp, err := http.Post("http://127.0.0.1:8000/whatsapp/incoming", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				fmt.Println("[ERROR] Failed to forward to Python:", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Println("[WARN] Python server returned status:", resp.StatusCode)
			} else {
				fmt.Println("[+] Forwarded message to Python server")
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
	client.SendMessage(context.Background(), jid, &waProto.Message{
		Conversation: proto.String(payload.Text),
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"sent"}`))
}

func parseJID(s string) (types.JID, bool) {
	jid, err := types.ParseJID(s)
	if err != nil {
		return types.JID{}, false
	}
	return jid, true
}

func main() {
	allowedNumber = getMasterNumber()
	if allowedNumber == "" {
		fmt.Println("[CRITICAL] Master Phone Number is not set or empty in config.toml! This is mandatory. Exiting...")
		os.Exit(1)
	} else {
		fmt.Println("[INFO] Loaded Master Phone Number:", allowedNumber)
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

	logger := waLog.Stdout("Main", "INFO", true)

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
				qrPath := filepath.Join(zarexDir, "qr.png")
				err := qrcode.WriteFile(evt.Code, qrcode.Medium, 256, qrPath)
				if err != nil {
					fmt.Println("[WARN] Failed to write QR image file:", err)
				} else {
					fmt.Println("[QR_IMAGE_READY]")
				}
			} else {
				fmt.Println("QR event:", evt.Event)
			}
		}
	} else {
		if err := client.Connect(); err != nil {
			panic(err)
		}
		fmt.Println("[+] Reconnected to WhatsApp")
	}

	http.HandleFunc("/send", sendHandler)
	fmt.Println("[+] HTTP bridge running on 127.0.0.1:8765")
	if err := http.ListenAndServe("127.0.0.1:8765", nil); err != nil {
		panic(err)
	}
}
