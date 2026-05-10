package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
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
	"github.com/gin-gonic/gin"
)

var apiGatewayURL string
var waClient *whatsmeow.Client

var userSessions = make(map[string]types.JID)

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
		if strings.HasPrefix(msgText, "?order") {
			re := regexp.MustCompile(`^\?order\s+(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*,\s*(jastip|anjem)\s*,\s*(.+)$`)
			matches := re.FindStringSubmatch(msgText)
			if matches == nil {
				sendMessage(client, senderJID, "Invalid format.\n\nUsage:\n?order <origin_lat>,<origin_lng>,<dest_lat>,<dest_lng>,<service_type>,<request_notes>\n\nExample:\n?order -6.9175,107.6191,-6.9260,107.6105,jastip,Beliin kopi Starbucks")
				return
			}

			originLat, err1 := strconv.ParseFloat(matches[1], 64)
			originLng, err2 := strconv.ParseFloat(matches[2], 64)
			destLat, err3 := strconv.ParseFloat(matches[3], 64)
			destLng, err4 := strconv.ParseFloat(matches[4], 64)
			if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
				sendMessage(client, senderJID, "Invalid coordinates. Please provide valid latitude/longitude values.")
				return
			}

			serviceType := matches[5]
			requestNotes := strings.TrimSpace(matches[6])

			sendMessage(client, senderJID, "Creating your order...")
			userID := getUserByPhone(sender)
			if userID == "" {
				sendMessage(client, senderJID, "Failed to find your account. Please make sure you are registered.")
				return
			}

			userSessions[userID] = senderJID

			orderID, status := createOrder(userID, originLat, originLng, destLat, destLng, serviceType, requestNotes)
			if orderID == "" {
				sendMessage(client, senderJID, "Failed to create order. Please try again.")
				return
			}

			sendMessage(client, senderJID, fmt.Sprintf("Order created! \xe2\x9c\x85\n\nID: %s\nStatus: %s\nService: %s\nNotes: %s\n\nSearching for a driver...", orderID, status, serviceType, requestNotes))
			return
		}

		switch msgText {
		case "?ready":
			if state.IsDriver {
				if updateDriverStatus(sender, true) {
					state.IsOnline = true
					sendMessage(client, senderJID, "You are now *READY* \xf0\x9f\x9f\x9f\xa2\nWaiting for orders...")
				} else {
					sendMessage(client, senderJID, "Failed to update status. \xe2\x9d\x8c")
				}
				return
			}
		case "?unready":
			if state.IsDriver {
				if updateDriverStatus(sender, false) {
					state.IsOnline = false
					sendMessage(client, senderJID, "You are now *NOT READY* \xf0\x9f\x94\xb4")
				} else {
					sendMessage(client, senderJID, "Failed to update status. \xe2\x9d\x8c")
				}
				return
			}
		case "?help":
			helpText := "*Available Commands:*\n\n?order <lat>,<lng>,<lat>,<lng>,<type>,<notes>\n  Create a new order\n\n?ready\n  Go online (drivers only)\n\n?unready\n  Go offline (drivers only)"
			sendMessage(client, senderJID, helpText)
			return
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

func getUserByPhone(phone string) string {
	url := fmt.Sprintf("%s/users/%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error getting user by phone: %v\n", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var user struct {
			ID string `json:"id"`
		}
		json.NewDecoder(resp.Body).Decode(&user)
		return user.ID
	}
	return ""
}

func createOrder(userID string, originLat, originLng, destLat, destLng float64, serviceType, requestNotes string) (string, string) {
	url := fmt.Sprintf("%s/orders", apiGatewayURL)

	payload, _ := json.Marshal(map[string]interface{}{
		"user_id":         userID,
		"origin_lat":      originLat,
		"origin_lng":      originLng,
		"destination_lat": destLat,
		"destination_lng": destLng,
		"service_type":    serviceType,
		"request_notes":   requestNotes,
	})

	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error creating order: %v\n", err)
		return "", ""
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		var order struct {
			ID     string  `json:"id"`
			Status string  `json:"status"`
			FinalPrice float64 `json:"final_price"`
		}
		json.NewDecoder(resp.Body).Decode(&order)
		return order.ID, order.Status
	}
	return "", ""
}

func handleOrderAccepted(c *gin.Context) {
	var payload struct {
		OrderID    string      `json:"order_id"`
		UserID     string      `json:"user_id"`
		DriverID   interface{} `json:"driver_id"`
		FinalPrice float64     `json:"final_price"`
		Status     string      `json:"status"`
		ServiceType string    `json:"service_type"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jid, exists := userSessions[payload.UserID]
	if !exists {
		fmt.Printf("no JID found for user_id: %s\n", payload.UserID)
		c.JSON(http.StatusOK, gin.H{"message": "no session found"})
		return
	}

	msg := fmt.Sprintf("Driver found! \xe2\x9c\x85\n\nOrder: %s\nService: %s\nFee: Rp %.0f\nStatus: Driver is on the way",
		payload.OrderID, payload.ServiceType, payload.FinalPrice)

	sendMessage(waClient, jid, msg)

	c.JSON(http.StatusOK, gin.H{"message": "notification sent"})
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
	waClient = client
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

	callbackPort := config.GetEnv("CALLBACK_PORT", "9084")
	gin.SetMode(gin.ReleaseMode)
	callbackRouter := gin.Default()
	callbackRouter.POST("/callback/order-accepted", handleOrderAccepted)
	go func() {
		fmt.Printf("Callback server starting on port %s...\n", callbackPort)
		if err := callbackRouter.Run(":" + callbackPort); err != nil {
			log.Fatalf("callback server error: %v", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	client.Disconnect()
}
