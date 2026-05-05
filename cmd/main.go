package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/Endea4/studExE4-customer-bot/shared/config"
)

var apiGatewayURL string

// Simple in-memory state machine for the flow
type UserState struct {
	Step     int
	Name     string
	IsDriver bool
	IsOnline bool
}

const (
	StepNormal    = 0
	StepAskName   = 1
	StepAskGender = 2
)

var userStates = make(map[string]*UserState)

func randomDelay() {
	// Random delay between 2 to 7 seconds (2000ms + 0-5000ms) to avoid ban
	delay := 2000 + rand.Intn(5000)
	time.Sleep(time.Duration(delay) * time.Millisecond)
}

func sendMessage(client *whatsmeow.Client, to types.JID, text string) {
	// Show "typing..." indicator
	client.SendChatPresence(context.Background(), to, types.ChatPresenceComposing, types.ChatPresenceMediaText)

	// Wait before sending to simulate human typing and avoid ban
	randomDelay()

	// Stop "typing..." indicator (optional, usually stops when message sent, but good practice)
	client.SendChatPresence(context.Background(), to, types.ChatPresencePaused, types.ChatPresenceMediaText)

	msg := &waProto.Message{
		Conversation: proto.String(text),
	}
	_, err := client.SendMessage(context.Background(), to, msg)
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
	}
}

func eventHandler(evt interface{}, client *whatsmeow.Client) {
	switch v := evt.(type) {
	case *events.Message:
		// Only accept messages from real users
		if v.Info.IsGroup || v.Info.IsFromMe {
			return
		}
		if v.Info.Sender.Server == "broadcast" || v.Info.Sender.Server == "newsletter" {
			return
		}

		senderJID := v.Info.Sender.ToNonAD()

		// Attempt to resolve LID to PN (Phone Number) if applicable
		if senderJID.Server == types.HiddenUserServer {
			pnJID, err := client.Store.LIDs.GetPNForLID(context.Background(), senderJID)
			if err == nil && !pnJID.IsEmpty() {
				fmt.Printf("Resolved LID %s to Phone Number %s\n", senderJID.User, pnJID.User)
				senderJID = pnJID
			} else {
				fmt.Printf("Could not resolve LID %s. Using LID as ID.\n", senderJID.User)
			}
		}

		sender := senderJID.User

		if sender == "0" {
			return
		}

		// Extract text from standard message or extended message (replies/links)
		msgText := v.Message.GetConversation()
		if msgText == "" {
			msgText = v.Message.GetExtendedTextMessage().GetText()
		}

		// Mark the message as read (blue tick)
		client.MarkRead(context.Background(), []types.MessageID{v.Info.ID}, v.Info.Timestamp, v.Info.Chat, v.Info.Sender)

		fmt.Printf("Message from %s: %s\n", sender, msgText)

		// Check if we are currently tracking this user's state
		state, exists := userStates[sender]
		if !exists {
			isNewUser := registerUser(sender)
			isDriver, isOnline := getDriverInfo(sender)

			state = &UserState{
				Step:     StepNormal,
				IsDriver: isDriver,
				IsOnline: isOnline,
			}
			userStates[sender] = state

			if isNewUser {
				state.Step = StepAskName
				sendMessage(client, senderJID, "Welcome to StudEx!\nTo get started, what is your *name*?")
			} else {
				sendMessage(client, senderJID, "Welcome back to StudEx! Where do you want to go today?")
				if isDriver {
					if isOnline {
						sendMessage(client, senderJID, "You are *READY* \xf0\x9F\x9F\xA2 waiting for orders. Type `?unready` to go offline.")
					} else {
						sendMessage(client, senderJID, "You are *NOT READY* \xf0\x9F\x94\xB4 — Type `?ready` to start receiving orders.")
					}
				}
			}
		return
	}

		// --- Command Handling ---
		switch msgText {
		case "?ready":
			if state.IsDriver {
				if updateDriverStatus(sender, true) {
					state.IsOnline = true
					sendMessage(client, senderJID, "You are now *READY* \xf0\x9f\x9F\xA2\nWaiting for orders...")
				} else {
					sendMessage(client, senderJID, "Failed to update status. \xe2\x9d\x8c")
				}
				return
			}
		case "?unready":
			if state.IsDriver {
				if updateDriverStatus(sender, false) {
					state.IsOnline = false
					sendMessage(client, senderJID, "You are now *NOT READY* \xf0\x9f\x94\xB4")
				} else {
					sendMessage(client, senderJID, "Failed to update status. \xe2\x9d\x8c")
				}
				return
			}
		}

		// Handle the conversational flow based on their current step
		switch state.Step {
		case StepAskName:
			state.Name = msgText
			state.Step = StepAskGender
			sendMessage(client, senderJID, fmt.Sprintf("Nice to meet you, %s! \xf0\x9f\x91\x8b\nLastly, what is your *gender*? (e.g., Male/Female)", state.Name))

		case StepAskGender:
			gender := msgText
			personalizeUser(sender, state.Name, state.Name, gender)

			state.Step = StepNormal
			sendMessage(client, senderJID, "Awesome! Your profile is complete. \xe2\x9c\x85\nYou are now ready to order a ride. (Type !help for commands)")

		case StepNormal:
			sendMessage(client, senderJID, "Welcome to StudEx! Where do you want to go today? \xf0\x9f\x93\x8d\n(Type `!help` for more options)")
			if state.IsDriver {
				if state.IsOnline {
					sendMessage(client, senderJID, "You are currently *READY* \xf0\x9f\x9F\xA2 — Type `?unready` to go offline.")
				} else {
					sendMessage(client, senderJID, "You are *NOT READY* \xf0\x9f\x94\xB4 — Type `?ready` to start receiving orders.")
				}
			}
		}
	}
}

// Returns true if a new user was created, false if they already exist
func registerUser(phone string) bool {
	url := fmt.Sprintf("%s/users/register", apiGatewayURL)

	payload, _ := json.Marshal(map[string]string{"phone": phone})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("Error calling user-service: %v\n", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		fmt.Printf("Successfully registered new user: %s\n", phone)
		return true
	}
	return false
}

func personalizeUser(phone, name, displayName, gender string) {
	url := fmt.Sprintf("%s/users/%s/personalize", apiGatewayURL, phone)

	payload, _ := json.Marshal(map[string]string{
		"name":         name,
		"display_name": displayName,
		"gender":       gender,
	})

	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error calling user-service: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("Successfully personalized user: %s\n", phone)
	}
}

func getDriverInfo(phone string) (bool, bool) {
	url := fmt.Sprintf("%s/drivers/me?phone=%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var driver struct {
			PlateNumber string `json:"plate_number"`
			Status      string `json:"status"`
		}
		json.NewDecoder(resp.Body).Decode(&driver)
		return driver.PlateNumber != "", driver.Status == "ready"
	}
	return false, false
}

func updateDriverStatus(phone string, online bool) bool {
	url := fmt.Sprintf("%s/drivers/me/status?phone=%s", apiGatewayURL, phone)
	status := "offline"
	if online {
		status = "ready"
	}
	payload, _ := json.Marshal(map[string]interface{}{"status": status})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func main() {
	config.LoadConfig()
	apiGatewayURL = config.GetEnv("API_GATEWAY_URL", "http://localhost:8080")

	dbLog := waLog.Stdout("Database", "DEBUG", true)
	container, err := sqlstore.New(context.Background(), "sqlite3", "file:examplestore.db?_foreign_keys=on", dbLog)
	if err != nil {
		panic(err)
	}

	deviceRes, err := container.GetFirstDevice(context.Background())
	if err != nil {
		panic(err)
	}

	clientLog := waLog.Stdout("Client", "DEBUG", true)
	client := whatsmeow.NewClient(deviceRes, clientLog)
	client.AddEventHandler(func(evt interface{}) {
		eventHandler(evt, client)
	})

	if client.Store.ID == nil {
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			panic(err)
		}
		for evt := range qrChan {
			if evt.Event == "code" {
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				fmt.Println("QR code generated. Please scan with your WhatsApp app.")
			} else {
				fmt.Println("QR channel event:", evt.Event)
			}
		}
	} else {
		err = client.Connect()
		if err != nil {
			panic(err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	client.Disconnect()
}
