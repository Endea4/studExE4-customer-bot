package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal/v3"
	"github.com/redis/go-redis/v9"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/Endea4/studExE4-customer-bot/internal/shared/config"
	"github.com/Endea4/studExE4-customer-bot/internal/shared/events"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func logErr(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

var apiGatewayURL string
var wabaURL string
var webAppBaseURL string

var waClient *whatsmeow.Client
var activeTripPartner = make(map[string]string)
var lastCustomerMessage = make(map[string]time.Time)

const (
	StepNormal          = 0
	StepAskName         = 1
	StepAskNickname     = 2
	StepAskGender       = 3
	StepAskPickup       = 4
	StepAskDest         = 5
	StepAskServiceType  = 6
	StepAskCustomReq    = 7
	StepAskItemDetail   = 8
	StepConfirm         = 9
	StepAskSaveLocation = 10
)

type UserState struct {
	Step            int
	Name            string
	Nickname        string
	IsDriver        bool
	IsOnline        bool
	UserID          string
	PickupLat       float64
	PickupLng       float64
	DestLat         float64
	DestLng         float64
	PickupSet       bool
	DestSet         bool
	ActiveTrip      string
	ServiceType     string
	CustomRequests  string
	ItemDetail      string
	GenderPref      string
	LastPickupLat   float64
	LastPickupLng   float64
	LastDestLat     float64
	LastDestLng     float64
	LastServiceType string
	ChatSessionID   string
	CurrentBidPrice float64
	LastBidder      string
	LastMsgAt       map[string]time.Time
	HasDebt         bool
	DebtAmount      float64
	MenuContext     string
	PendingAction   string
	MatchedCustomerPhone string
}

var userStates = make(map[string]*UserState)
var gpsRelayActive atomic.Int32

// ─── Known accounts: pre-seeded, skip registration ───
func randomDelay() {
	base := 500 + rand.Intn(1500)
	jitter := rand.Intn(1000)
	time.Sleep(time.Duration(base+jitter) * time.Millisecond)
}

func humanPause() {
	if rand.Intn(3) == 0 {
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)
	}
}

func sendMessage(client *whatsmeow.Client, to types.JID, text string) {
	preview := text
	if len(preview) > 80 {
		preview = preview[:80] + "..."
	}
		fmt.Printf("[SEND] to=%s text=%q\n", to.User, preview)
	logWAChat("out", to.User, "WHATSMEOW", text)
	client.SendChatPresence(context.Background(), to, types.ChatPresenceComposing, types.ChatPresenceMediaText)
	humanPause()
	randomDelay()
	client.SendChatPresence(context.Background(), to, types.ChatPresencePaused, types.ChatPresenceMediaText)
	msg := &waProto.Message{Conversation: proto.String(text)}
	_, err := client.SendMessage(context.Background(), to, msg)
	if err != nil {
		fmt.Printf("[SEND-FAIL] to=%s err=%v\n", to.User, err)
	}
}

func relayMessage(client *whatsmeow.Client, to types.JID, text string) {
	fmt.Printf("[RELAY] to=%s text=%q\n", to.User, text)
	logWAChat("out", to.User, "WHATSMEOW", text)
	client.SendChatPresence(context.Background(), to, types.ChatPresenceComposing, types.ChatPresenceMediaText)
	randomDelay()
	client.SendChatPresence(context.Background(), to, types.ChatPresencePaused, types.ChatPresenceMediaText)
	msg := &waProto.Message{Conversation: proto.String(text)}
	_, err := client.SendMessage(context.Background(), to, msg)
	if err != nil {
		fmt.Printf("[RELAY-FAIL] to=%s err=%v\n", to.User, err)
	}
}

func relayMedia(client *whatsmeow.Client, from types.JID, to types.JID, msg *waProto.Message, mediaType string) {
	fmt.Printf("[RELAY-MEDIA] from=%s to=%s type=%s\n", from.User, to.User, mediaType)
	logWAChat("out", to.User, "WHATSMEOW", "[media] "+mediaType)
	client.SendChatPresence(context.Background(), to, types.ChatPresenceComposing, types.ChatPresenceMediaText)
	randomDelay()
	client.SendChatPresence(context.Background(), to, types.ChatPresencePaused, types.ChatPresenceMediaText)
	_, err := client.SendMessage(context.Background(), to, msg)
	if err != nil {
		fmt.Printf("[RELAY-MEDIA-FAIL] to=%s type=%s err=%v\n", to.User, mediaType, err)
	}
}

func sendMessages(client *whatsmeow.Client, to types.JID, texts []string) {
	for _, text := range texts {
		sendMessage(client, to, text)
	}
}

func sendLocation(client *whatsmeow.Client, to types.JID, lat, lng float64, name string) {
	fmt.Printf("[SEND-LOC] to=%s lat=%.4f lng=%.4f name=%s\n", to.User, lat, lng, name)
	logWAChat("out", to.User, "WHATSMEOW", fmt.Sprintf("[location] %.4f,%.4f %s", lat, lng, name))
	msg := &waProto.Message{
		LocationMessage: &waProto.LocationMessage{
			DegreesLatitude:  proto.Float64(lat),
			DegreesLongitude: proto.Float64(lng),
			Name:             proto.String(name),
		},
	}
	_, err := client.SendMessage(context.Background(), to, msg)
	if err != nil {
		fmt.Printf("[SEND-LOC-FAIL] to=%s err=%v\n", to.User, err)
	}
}

func sendButtons(client *whatsmeow.Client, to types.JID, text string, buttons []string) {
	sendMessage(client, to, text)
}

func eventHandler(evt interface{}, client *whatsmeow.Client) {
	switch v := evt.(type) {
	case *waEvents.Message:
		if v.Info.IsGroup || v.Info.IsFromMe {
			return
		}
		if v.Info.Sender.Server == "broadcast" || v.Info.Sender.Server == "newsletter" {
			return
		}

		senderJID := v.Info.Sender.ToNonAD()
		if senderJID.Server == types.HiddenUserServer {
			pnJID, err := client.Store.LIDs.GetPNForLID(context.Background(), senderJID)
			if err == nil && !pnJID.IsEmpty() {
				senderJID = pnJID
			}
		}

		sender := senderJID.User
		if sender == "0" {
			return
		}

		client.MarkRead(context.Background(), []types.MessageID{v.Info.ID}, v.Info.Timestamp, v.Info.Chat, v.Info.Sender)

		msgText := v.Message.GetConversation()
		if msgText == "" {
			msgText = v.Message.GetExtendedTextMessage().GetText()
		}
		if strings.HasPrefix(msgText, "!") {
			msgText = "?" + msgText[1:]
		}

		locMsg := v.Message.GetLocationMessage()
		imgMsg := v.Message.GetImageMessage()
		vidMsg := v.Message.GetVideoMessage()
		audMsg := v.Message.GetAudioMessage()
		docMsg := v.Message.GetDocumentMessage()
		stkMsg := v.Message.GetStickerMessage()

		fmt.Printf("[RECV] from=%s: %q (hasLocation=%v hasImage=%v hasVideo=%v hasAudio=%v hasDoc=%v hasSticker=%v)\n", sender, msgText, locMsg != nil, imgMsg != nil, vidMsg != nil, audMsg != nil, docMsg != nil, stkMsg != nil)

		isDriver, isOnline := getDriverInfo(sender)
		state, exists := loadUserState(sender)

		if !exists {
			userID := registerUser(sender)
			state = &UserState{
				Step:     StepNormal,
				IsDriver: isDriver,
				IsOnline: isOnline,
				UserID:   userID,
			}
			saveUserState(sender, state)
		} else {
			state.IsDriver = isDriver
			state.IsOnline = isOnline
		}
		defer saveUserState(sender, state)

		var recvRole string
		if isDriver {
			recvRole = "DRIVER"
		} else if state.UserID != "" && state.UserID != sender {
			recvRole = "CUSTOMER"
		} else {
			userID := resolveUserID(sender)
			if userID != "" {
				recvRole = "CUSTOMER"
			} else {
				recvRole = "NEW"
			}
		}
		if msgText != "" {
			logWAChat("in", sender, recvRole, msgText)
		}
		if locMsg != nil {
			logWAChat("in", sender, recvRole, fmt.Sprintf("[location] %.4f,%.4f", locMsg.GetDegreesLatitude(), locMsg.GetDegreesLongitude()))
		}
		if imgMsg != nil {
			logWAChat("in", sender, recvRole, "[image]")
		}
		if vidMsg != nil {
			logWAChat("in", sender, recvRole, "[video]")
		}
		if audMsg != nil {
			logWAChat("in", sender, recvRole, "[audio]")
		}
		if docMsg != nil {
			logWAChat("in", sender, recvRole, fmt.Sprintf("[document] %s", docMsg.GetFileName()))
		}
		if stkMsg != nil {
			logWAChat("in", sender, recvRole, "[sticker]")
		}

		cmd := msgText
		if idx := strings.IndexByte(msgText, ' '); idx > 0 {
			cmd = msgText[:idx]
		}

		if state.PendingAction != "" {
			handled := handlePendingAction(client, sender, senderJID, state, msgText)
			if handled {
				return
			}
		}

		if state.MenuContext != "" && len(cmd) == 1 && ((cmd[0] >= 'a' && cmd[0] <= 'z') || (cmd[0] >= 'A' && cmd[0] <= 'Z')) {
			handled := handleMenuLetter(client, sender, senderJID, state, strings.ToLower(cmd), strings.TrimSpace(msgText[len(cmd):]))
			if handled {
				return
			}
		}

		if partner, linked := loadPartner(sender); linked {
			isMedia := imgMsg != nil || vidMsg != nil || audMsg != nil || docMsg != nil || stkMsg != nil
			isText := msgText != "" && !strings.HasPrefix(msgText, "?") && !strings.HasPrefix(msgText, "!")
			isLoc := locMsg != nil

			if isMedia || isText || isLoc {
				relayRole := "customer"
				if isDriver {
					relayRole = "driver"
				}

				if isMedia {
					relayMedia(client, senderJID, waJID(partner), v.Message, "media")
					saveChatMessage(sender, relayRole, "media", fmt.Sprintf("[media]"))
				} else if isLoc {
					relayMedia(client, senderJID, waJID(partner), v.Message, "location")
					saveChatMessage(sender, relayRole, "location", fmt.Sprintf("[location] %.4f,%.4f", locMsg.GetDegreesLatitude(), locMsg.GetDegreesLongitude()))
				} else {
					relayMessage(client, waJID(partner), fmt.Sprintf("[%s]: %s", strings.Title(relayRole), msgText))
					saveChatMessage(sender, relayRole, "text", msgText)
				}
				saveLastMsg(partner, time.Now())
				return
			}
		}

		if !exists {
			userInfo := getUserInfo(sender)
			if userInfo != nil && userInfo["name"] != nil && userInfo["name"].(string) != "" && userInfo["name"].(string) != sender {
				if isDriver {
					if isOnline {
						sendMessage(client, senderJID, "Welcome back! You are *READY* waiting for orders.\nType `?unready` to go offline.")
					} else {
						sendMessage(client, senderJID, "Welcome back! You are *NOT READY*.\nType `?ready` to go online, or share a *pickup location* to order a ride.")
					}
				} else {
					sendMessage(client, senderJID, "Welcome back! Send your *pickup location* (WA share location) to order a ride.")
				}
				return
			}

			state.Step = StepAskName
			sendMessage(client, senderJID, "Welcome to StudEx!\nTo get started, what is your *name*?")
			return
		}

	switch cmd {
		case "?ready":
			if state.IsDriver {
				check := checkDriverGpsAndLocation(sender)
				fmt.Printf("[?ready] driverID=%s location=%v gps=%v\n", check.refID, check.location != nil, check.gpsActive)
				if check.refID == "" {
					sendMessage(client, senderJID, "Cannot go ready — no driver profile found.\nMake sure you are registered.")
					return
				}
				if !check.gpsActive {
					sendMessage(client, senderJID, "Cannot go ready — *GPS is off*.\nTurn on GPS location sharing in the app first, then try `?ready` again.")
					return
				}
				markDriverOnline(check.refID)
				if check.location != nil {
					lat, _ := check.location["latitude"].(float64)
					lng, _ := check.location["longitude"].(float64)
					registerDriverLocation(check.refID, lat, lng)
					addCandidateToPool(check.refID, lat, lng)
				}
				if updateDriverStatus(sender, true) {
					state.IsOnline = true
					if check.location != nil {
						sendMessage(client, senderJID, fmt.Sprintf("You are now *READY*\nLocation: %.4f, %.4f\nWaiting for orders...\nType `?unready` to stop receiving orders.", check.location["latitude"], check.location["longitude"]))
					} else {
						sendMessage(client, senderJID, "You are now *READY*\nSend your location from the app to become matchable.\nType `?unready` to stop receiving orders.")
					}
				} else {
					sendMessage(client, senderJID, "Failed to update status.")
				}
				return
			}
		case "?unready":
			if state.IsDriver {
				reason := ""
				if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
					reason = strings.TrimSpace(parts[1])
				}
				check := checkDriverGpsAndLocation(sender)
				fmt.Printf("[?unready] driverID=%s reason=%s\n", check.refID, reason)
				if check.refID != "" {
					markDriverOffline(check.refID)
				}
				if updateDriverStatus(sender, false) {
					state.IsOnline = false
					msg := "You are now *NOT READY*\nYou can also order a ride by sharing a pickup location."
					if reason != "" {
						msg += fmt.Sprintf("\nReason: %s", reason)
					}
					sendMessage(client, senderJID, msg)
				} else {
					sendMessage(client, senderJID, "Failed to update status.")
				}
				return
			}
		case "!help":
			if state.IsDriver {
				sendMessage(client, senderJID, "*Driver Commands:*\n?ready — Go online\n?unready [reason] — Go offline\n?bid <amount> <reason> — Bid a price\n?deal — Accept current price\n?start — Start trip (pickup done)\n?abort <reason> — Emergency abort\n?cancel <reason> — Cancel trip\n?status — Check status\n?history <n> — Order history\n?reputation <n> — Reviews received\n?debt list — List debts\n?debt y/n <id> — Confirm/deny debt\n?izin <reason] — Request permission\n?cuti <reason] — Request leave\n\nWhen matched:\n  q: Accept\n  w: Reject\n\nAfter trip:\n  q: Rate (q 1-5)\n  w: Skip")
			} else {
				sendMessage(client, senderJID, "*Customer Commands:*\nShare *pickup location* (WA pin) to start a ride\n?cancel <reason> — Cancel request\n?status — Check ride status\n?retry — Retry last search\n?history <n> — Order history\n?bid <amount> <reason> — Bid a price\n?deal — Accept current price\n\nWhen confirming ride:\n  q: Go\n  w: Cancel\n\nAfter trip:\n  q: Rate (q 1-5)\n  w: Skip\n\nWhen in debt:\n  q: Pay\n  w: Confirm paid")
			}
			return
		case "?cancel":
			reason := ""
			if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
				reason = parts[1]
			}
			if reason == "" {
				sendMessage(client, senderJID, "Usage: `?cancel <reason>`\nExample: ?cancel tidak jadi")
				return
			}
			if state.IsDriver && state.ActiveTrip != "" {
				trip := getDriverActiveTrip(sender)
				if trip != nil {
					cancelTrip(trip["id"].(string), reason, "driver", sender)
					deletePartner(sender)
					if custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"])); custPhone != "" {
						deletePartner(custPhone)
						sendMessage(client, waJID(custPhone), "Driver cancelled the trip. Searching for another driver...")
					}
					closeChatSession(trip["id"].(string))
				}
			} else if !state.IsDriver && state.ActiveTrip != "" {
				tripData := getCustomerActiveTrip(sender)
				if tripData != nil {
					cancelTrip(tripData["id"].(string), reason, "customer", sender)
					if drvPhone := resolvePhone(fmt.Sprintf("%v", tripData["driver_ref_id"])); drvPhone != "" {
						deletePartner(drvPhone)
						sendMessage(client, waJID(drvPhone), "Customer cancelled the trip.")
					}
					closeChatSession(tripData["id"].(string))
				}
				deletePartner(sender)
			} else {
				deletePartner(sender)
			}
			state.ActiveTrip = ""
			state.CurrentBidPrice = 0
			state.LastBidder = ""
			state.Step = StepNormal
			state.PickupSet = false
			state.DestSet = false
			sendMessage(client, senderJID, "Request cancelled.")
			return
		case "?bid":
			params := strings.TrimSpace(strings.TrimPrefix(msgText, "?bid"))
			if params == "" {
				sendMessage(client, senderJID, "Usage: *?bid <amount> <reason>*\nExample: ?bid 18000 jauh pak")
				return
			}
			doBid(client, sender, senderJID, state, params)
			return
		case "?deal":
			trip := getDriverActiveTrip(sender)
			if trip == nil && !state.IsDriver {
				trip = getCustomerActiveTrip(sender)
			}
			if trip == nil {
				trip = getCustomerActiveTrip(sender)
			}
			if trip != nil {
				status := trip["status"].(string)
				if state.IsDriver && status == "pending_acceptance" {
					if acceptTrip(trip["id"].(string)) {
						state.ActiveTrip = trip["id"].(string)
						state.LastBidder = ""
						custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
						if custPhone != "" && strings.HasPrefix(custPhone, "62") {
							savePartner(sender, custPhone)
							savePartner(custPhone, sender)
						}
						sessionID := saveChatSession(trip["id"].(string), trip["order_id"].(string), sender, custPhone)
						state.ChatSessionID = sessionID
						if custState, ok := loadUserState(custPhone); ok && custState != nil {
							custState.ChatSessionID = sessionID
							custState.CurrentBidPrice = state.CurrentBidPrice
							custState.LastBidder = ""
							saveUserState(custPhone, custState)
						}
						sendMessage(client, senderJID, fmt.Sprintf("Trip accepted! Rp %.0f\n\n?bid <amount> <reason> — Bid a price\n?start — Start trip", state.CurrentBidPrice))
						if custPhone != "" {
							notifyWABA(custPhone, "driver_accepted", state.CurrentBidPrice, state.ActiveTrip)
						}
						saveChatMessage(sender, "driver", "system", "Trip accepted")
						saveChatMessage(custPhone, "customer", "system", "Trip accepted")
						state.MenuContext = "trip"
					} else {
						sendMessage(client, senderJID, "Failed to accept trip.")
					}
				} else if state.IsDriver && (status == "bargaining" || status == "accepted") {
					if dealTrip(trip["id"].(string)) {
						custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
						if custPhone != "" && strings.HasPrefix(custPhone, "62") {
							savePartner(sender, custPhone)
							savePartner(custPhone, sender)
						}
						sessionID := saveChatSession(trip["id"].(string), trip["order_id"].(string), sender, custPhone)
						state.ChatSessionID = sessionID
						if custState, ok := loadUserState(custPhone); ok && custState != nil {
							custState.ChatSessionID = sessionID
							custState.CurrentBidPrice = state.CurrentBidPrice
							custState.LastBidder = ""
							saveUserState(custPhone, custState)
						}
						sendMessage(client, senderJID, fmt.Sprintf("Deal! Rp %.0f\n\n?start when you pick up the customer", state.CurrentBidPrice))
						if custPhone != "" {
							sendMessage(client, waJID(custPhone), fmt.Sprintf("Driver confirmed deal! Rp %.0f\nWaiting for driver to start the trip...", state.CurrentBidPrice))
						}
						state.LastBidder = ""
						saveChatMessage(sender, "driver", "system", "Deal confirmed")
						saveChatMessage(custPhone, "customer", "system", "Deal confirmed")
					} else {
						sendMessage(client, senderJID, "Failed to confirm deal.")
					}
				} else if !state.IsDriver && (status == "bargaining" || status == "accepted") {
					if dealTrip(trip["id"].(string)) {
						drvPhone := resolvePhone(fmt.Sprintf("%v", trip["driver_ref_id"]))
						if drvPhone != "" && strings.HasPrefix(drvPhone, "62") {
							savePartner(sender, drvPhone)
							savePartner(drvPhone, sender)
						}
						if drvState, ok := loadUserState(drvPhone); ok && drvState != nil {
							drvState.CurrentBidPrice = state.CurrentBidPrice
							drvState.LastBidder = ""
							saveUserState(drvPhone, drvState)
						}
						sendMessage(client, senderJID, fmt.Sprintf("Deal confirmed! Rp %.0f\nWaiting for driver to start the trip...", state.CurrentBidPrice))
						if drvPhone != "" {
							sendMessage(client, waJID(drvPhone), fmt.Sprintf("Customer confirmed deal! Rp %.0f\n\n?start when you pick up the customer", state.CurrentBidPrice))
						}
						state.LastBidder = ""
						saveChatMessage(sender, "customer", "system", "Deal confirmed")
						saveChatMessage(drvPhone, "driver", "system", "Deal confirmed")
					} else {
						sendMessage(client, senderJID, "Failed to confirm deal.")
					}
				} else {
					sendMessage(client, senderJID, fmt.Sprintf("Cannot deal — trip status is %s.", status))
				}
			} else {
				sendMessage(client, senderJID, "No active trip to deal.")
			}
			return
		case "?start":
			if state.IsDriver {
				trip := getDriverActiveTrip(sender)
				if trip != nil && (trip["status"].(string) == "deal" || trip["status"].(string) == "accepted") {
					if state.LastBidder == "" && state.CurrentBidPrice <= 0 {
						sendMessage(client, senderJID, "Please *?bid <amount> <reason>* first to set a price, then ?start.")
						return
					}
				if startTrip(trip["id"].(string)) {
					sendMessage(client, senderJID, "Trip started!\n\nq: Complete (Paid)\nw: Complete (Debt)")
					state.MenuContext = "complete"
				} else {
						sendMessage(client, senderJID, "Failed to start trip.")
					}
				} else {
					sendMessage(client, senderJID, "No deal/accepted trip to start.")
				}
			}
			return
		case "?complete":
			if !state.IsDriver {
				sendMessage(client, senderJID, "Only drivers can complete trips.")
				return
			}
			parts := strings.SplitN(msgText, " ", 2)
			payStatus := ""
			if len(parts) > 1 {
				payStatus = strings.TrimSpace(parts[1])
			}
			if payStatus != "paid" && payStatus != "debt" {
				sendMessage(client, senderJID, "Usage: *?complete paid* or *?complete debt*")
				return
			}
			trip := getDriverActiveTrip(sender)
			if trip == nil || trip["status"].(string) != "in_progress" {
				sendMessage(client, senderJID, "No in-progress trip to complete.")
				return
			}
			if payStatus == "debt" {
				state.PendingAction = "complete_debt_awaiting_amount"
				tripValue, _ := trip["final_price"].(float64)
				if tripValue == 0 {
					tripValue, _ = trip["current_bid_price"].(float64)
				}
				sendMessage(client, senderJID, fmt.Sprintf("Paid amount? (trip value: Rp %.0f)", tripValue))
				return
			}
			if completeTrip(trip["id"].(string), "paid") {
				state.ActiveTrip = ""
				state.ChatSessionID = ""
				state.CurrentBidPrice = 0
				state.LastBidder = ""
				state.MenuContext = "rate"
				state.PendingAction = "auto_ready_after_rate"
				sendMessage(client, senderJID, "Trip completed! (Paid)\n\nq: Rate (q 1-5)\nw: Skip")
				go func() {
					time.Sleep(2 * time.Second)
					check := checkDriverGpsAndLocation(sender)
					if check.refID != "" {
						if check.gpsActive && check.location != nil {
							markDriverOnline(check.refID)
							lat, _ := check.location["latitude"].(float64)
							lng, _ := check.location["longitude"].(float64)
							registerDriverLocation(check.refID, lat, lng)
							addCandidateToPool(check.refID, lat, lng)
						}
						updateDriverStatus(sender, true)
					}
					drvState, ok := loadUserState(sender)
					if ok && drvState != nil {
						drvState.IsOnline = true
						drvState.MenuContext = "rate"
						drvState.PendingAction = "auto_ready_after_rate"
						saveUserState(sender, drvState)
					}
				}()
			} else {
				sendMessage(client, senderJID, "Failed to complete trip.")
			}
			return
		case "?retry":
			if state.IsDriver {
				sendMessage(client, senderJID, "Drivers cannot use ?retry.")
				return
			}
			if state.LastPickupLat == 0 && state.LastPickupLng == 0 {
				sendMessage(client, senderJID, "No previous ride to retry. Share your *pickup location* to start a new ride.")
				return
			}
			if state.ActiveTrip != "" {
				activeTrip := getCustomerActiveTrip(sender)
				if activeTrip != nil {
					sendMessage(client, senderJID, "You have an active trip. Type `?cancel <reason>` first.")
					return
				}
				cancelMatchRequest(state.ActiveTrip)
				state.ActiveTrip = ""
				state.CurrentBidPrice = 0
				state.LastBidder = ""
				state.MatchedCustomerPhone = ""
			}
			svcType := state.LastServiceType
			if svcType == "" {
				svcType = "anjem"
			}
			sendMessage(client, senderJID, fmt.Sprintf("Retrying last ride...\nPickup: %.4f, %.4f\nDest: %.4f, %.4f\nService: %s", state.LastPickupLat, state.LastPickupLng, state.LastDestLat, state.LastDestLng, serviceName(svcType)))
			result := requestRide(sender, state.LastPickupLat, state.LastPickupLng, state.LastDestLat, state.LastDestLng, svcType)
			if result != nil {
				orderID, _ := result["order_id"].(string)
				var link string
				if orderID != "" {
					link = buildTripLink(sender, orderID, state.LastPickupLat, state.LastPickupLng, state.LastDestLat, state.LastDestLng, svcType)
				}
				if link != "" {
					sendMessage(client, senderJID, "Pesanan kamu sedang dicari driver! Pantau, tawar harga, chat, dan lacak driver di sini:\n"+link)
				} else {
					sendMessage(client, senderJID, "Search started! Waiting for a driver match...")
				}
			} else {
				sendMessage(client, senderJID, "Failed to request ride. Type *?retry* to try again.")
			}
			return
		case "?status":
			if state.IsDriver {
				trip := getDriverActiveTrip(sender)
				if trip != nil {
					sendMessage(client, senderJID, fmt.Sprintf("Active trip: %s\nStatus: %s\nOrder: %s", trip["id"].(string), trip["status"].(string), trip["order_id"].(string)))
				} else {
					sendMessage(client, senderJID, "No active trip.")
				}
			} else {
				sendMessage(client, senderJID, "No active trip.")
			}
			return

		case "?abort":
			if state.IsDriver {
				reason := ""
				if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
					reason = strings.TrimSpace(parts[1])
				}
				if reason == "" {
					sendMessage(client, senderJID, "Usage: `?abort <reason>`")
					return
				}
				trip := getDriverActiveTrip(sender)
				if trip == nil {
					sendMessage(client, senderJID, "No active trip to abort.")
					return
				}
				custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
				lastMsg, ok := loadLastMsg(sender)
				if !ok || time.Since(lastMsg) < 10*time.Minute {
					sendMessage(client, senderJID, "Cannot abort — customer responded within the last 10 minutes.")
					return
				}
				if abortTrip(trip["id"].(string), reason, sender) {
					deletePartner(sender)
					deletePartner(custPhone)
					closeChatSession(trip["id"].(string))
					state.ActiveTrip = ""
					state.CurrentBidPrice = 0
					state.LastBidder = ""
					sendMessage(client, senderJID, "Trip aborted (customer unresponsive).")
					if custPhone != "" {
						sendMessage(client, waJID(custPhone), "Trip dibatalkan — driver abort (customer tidak merespon).")
					}
				} else {
					sendMessage(client, senderJID, "Failed to abort trip.")
				}
			}
			return



		case "?history":
			n := 5
			if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &n)
				if n <= 0 {
					n = 5
				}
			}
			if state.IsDriver {
				trips, err := getDriverRecentTrips(sender, n)
				if err != nil || len(trips) == 0 {
					sendMessage(client, senderJID, "No order history.")
					return
				}
				var lines []string
				for _, t := range trips {
					svc, _ := t["service_type"].(string)
					if svc == "" {
						svc = "anjem"
					}
					status, _ := t["status"].(string)
					price, _ := t["final_price"].(float64)
					lines = append(lines, fmt.Sprintf("- %s | %s | Rp %.0f | %s", t["order_id"], serviceName(svc), price, status))
				}
				sendMessage(client, senderJID, fmt.Sprintf("*Last %d orders:*\n%s", len(trips), strings.Join(lines, "\n")))
			} else {
				trips, err := getCustomerRecentTrips(sender, n)
				if err != nil || len(trips) == 0 {
					sendMessage(client, senderJID, "No order history.")
					return
				}
				var lines []string
				for _, t := range trips {
					svc, _ := t["service_type"].(string)
					if svc == "" {
						svc = "anjem"
					}
					status, _ := t["status"].(string)
					price, _ := t["final_price"].(float64)
					lines = append(lines, fmt.Sprintf("- %s | %s | Rp %.0f | %s", t["order_id"], serviceName(svc), price, status))
				}
				sendMessage(client, senderJID, fmt.Sprintf("*Last %d orders:*\n%s", len(trips), strings.Join(lines, "\n")))
			}
			return
		case "?reputation":
			if state.IsDriver {
				n := 5
				if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
					fmt.Sscanf(parts[1], "%d", &n)
				}
				reviews := getReputation(sender, n)
				if len(reviews) == 0 {
					sendMessage(client, senderJID, "No reviews yet.")
					return
				}
				var lines []string
				for _, r := range reviews {
					lines = append(lines, fmt.Sprintf("- %d/5: %s", int(r["score"].(float64)), r["review"]))
				}
				sendMessage(client, senderJID, fmt.Sprintf("*Your reviews:*\n%s", strings.Join(lines, "\n")))
			}
			return
		case "?debt":
			if state.IsDriver {
				subCmd := "list"
				if parts := strings.SplitN(msgText, " ", 3); len(parts) > 1 {
					subCmd = parts[1]
				}
				switch subCmd {
				case "list":
					debts := getDriverDebts(sender)
					if len(debts) == 0 {
						sendMessage(client, senderJID, "No debts.")
						return
					}
					var lines []string
					for _, d := range debts {
						status, _ := d["status"].(string)
						amount, _ := d["amount"].(float64)
						remaining, _ := d["remaining"].(float64)
						id, _ := d["id"].(string)
						if id == "" {
							if oid, ok := d["_id"]; ok {
								id = fmt.Sprintf("%v", oid)
							}
						}
						lines = append(lines, fmt.Sprintf("- %s | Rp %.0f / %.0f | %s", id, remaining, amount, status))
					}
					sendMessage(client, senderJID, fmt.Sprintf("*Your debts:*\n%s\n\n`?debt y <id>` confirm\n`?debt n <id>` deny", strings.Join(lines, "\n")))
				case "y", "n":
					if len(strings.SplitN(msgText, " ", 3)) < 3 {
						sendMessage(client, senderJID, "Usage: `?debt y <id>` or `?debt n <id>`")
						return
					}
					parts := strings.SplitN(msgText, " ", 3)
					debtID := parts[2]
				if subCmd == "y" {
					confirmDebt(debtID)
					sendMessage(client, senderJID, "Debt payment confirmed.")
				} else {
					denyDebt(debtID)
					sendMessage(client, senderJID, "Debt marked as *disputed*. Admin will review.")
				}
				default:
					sendMessage(client, senderJID, "Usage: `?debt list` or `?debt y/n <id>`")
				}
			} else {
				sendMessage(client, senderJID, "Usage: `?debt list` or `?debt y/n <id>`")
			}
			return
		case "?izin":
			if state.IsDriver {
				reason := ""
				if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
					reason = strings.TrimSpace(parts[1])
				}
				if reason == "" {
					sendMessage(client, senderJID, "Usage: `?izin <reason>`")
					return
				}
				if submitLeaveRequest(sender, "izin", reason) {
					sendMessage(client, senderJID, "Izin request submitted. Waiting for admin approval.")
				} else {
					sendMessage(client, senderJID, "Failed to submit izin request.")
				}
			}
			return
		case "?cuti":
			if state.IsDriver {
				reason := ""
				if parts := strings.SplitN(msgText, " ", 2); len(parts) > 1 {
					reason = strings.TrimSpace(parts[1])
				}
				if reason == "" {
					sendMessage(client, senderJID, "Usage: `?cuti <reason>`")
					return
				}
				if submitLeaveRequest(sender, "cuti", reason) {
					sendMessage(client, senderJID, "Cuti request submitted. Waiting for admin approval.")
				} else {
					sendMessage(client, senderJID, "Failed to submit cuti request.")
				}
			}
			return
		}

		if locMsg != nil && (!state.IsDriver || !state.IsOnline) {
			if state.HasDebt {
				sendMessage(client, senderJID, fmt.Sprintf("You have *outstanding debt* of Rp %.0f. Please pay first.\n\nq: Pay\nw: Confirm paid\ne: Rate driver (e 1-5)", state.DebtAmount))
				state.MenuContext = "debt"
				return
			}
			lat := locMsg.GetDegreesLatitude()
			lng := locMsg.GetDegreesLongitude()

			switch state.Step {
			case StepNormal, StepAskPickup:
				state.PickupLat = lat
				state.PickupLng = lng
				state.PickupSet = true
				state.Step = StepAskDest
				sendMessage(client, senderJID, fmt.Sprintf("Pickup set: %.4f, %.4f\n\nNow share your *destination location*", lat, lng))

			case StepAskDest:
				state.DestLat = lat
				state.DestLng = lng
				state.DestSet = true
				state.Step = StepAskServiceType
				state.MenuContext = "service_type"
				sendMessage(client, senderJID, "Destination set!\n\nChoose *service type:*\nq: Anjem (escort ride)\nw: Jastip (delivery)")

			default:
				sendMessage(client, senderJID, "You have an active order. Type `?cancel` to cancel first.")
			}
			return
		}

		switch state.Step {
		case StepAskName:
			state.Name = msgText
			state.Step = StepAskNickname
			sendMessage(client, senderJID, fmt.Sprintf("Nice to meet you, %s!\nWhat should we call you? (*nickname*)", state.Name))

		case StepAskNickname:
			state.Nickname = msgText
			state.Step = StepAskGender
			state.MenuContext = "gender"
			sendMessage(client, senderJID, fmt.Sprintf("Got it, %s!\nWhat is your *gender*?\nq: Male\nw: Female", state.Nickname))

		case StepAskGender:
			sendMessage(client, senderJID, "q: Male\nw: Female")

		case StepAskServiceType:
			sendMessage(client, senderJID, "q: Anjem (escort ride)\nw: Jastip (delivery)")

		case StepAskCustomReq:
			if strings.ToLower(msgText) == "q" || strings.ToLower(msgText) == "skip" {
				state.CustomRequests = ""
				state.GenderPref = ""
			} else {
				extracted, _ := extractCustomRequest(msgText)
				state.GenderPref = extracted.GenderPref
				state.CustomRequests = extracted.Notes
			}
			state.Step = StepConfirm
			state.MenuContext = "confirm_ride"
			price := getPriceEstimate(state.PickupLat, state.PickupLng, state.DestLat, state.DestLng, state.ServiceType)
			detail := fmt.Sprintf("*%s*\nPickup: %.4f, %.4f\nDest: %.4f, %.4f", serviceName(state.ServiceType), state.PickupLat, state.PickupLng, state.DestLat, state.DestLng)
			if state.CustomRequests != "" {
				detail += fmt.Sprintf("\nRequest: %s", state.CustomRequests)
			}
			if state.GenderPref != "" {
				detail += fmt.Sprintf("\nGender pref: %s", state.GenderPref)
			}
			detail += fmt.Sprintf("\n\n*Price Estimate:* Rp %.0f\n*Distance:* %.1f km\n\nq: Go\nw: Cancel", price.Total, price.DistanceKm)
			sendMessage(client, senderJID, detail)

		case StepAskItemDetail:
			if strings.ToLower(msgText) == "q" || strings.ToLower(msgText) == "skip" {
				state.ItemDetail = ""
			} else {
				state.ItemDetail = msgText
			}
			state.Step = StepConfirm
			state.MenuContext = "confirm_ride"
			price := getPriceEstimate(state.PickupLat, state.PickupLng, state.DestLat, state.DestLng, state.ServiceType)
			detail := fmt.Sprintf("*%s*\nPickup: %.4f, %.4f\nDest: %.4f, %.4f\nBarang: %s", serviceName(state.ServiceType), state.PickupLat, state.PickupLng, state.DestLat, state.DestLng, state.ItemDetail)
			detail += fmt.Sprintf("\n\n*Price Estimate:* Rp %.0f\n*Distance:* %.1f km\n\nq: Go\nw: Cancel", price.Total, price.DistanceKm)
			sendMessage(client, senderJID, detail)

		case StepConfirm:
			sendMessage(client, senderJID, "q: Go\nw: Cancel")

		case StepAskPickup:
			sendMessage(client, senderJID, "Please share your *pickup location* using WhatsApp's share location feature (pin).")

		case StepAskDest:
			sendMessage(client, senderJID, "Please share your *destination location* using WhatsApp's share location feature (pin).")

		case StepAskSaveLocation:
			if strings.ToLower(msgText) == "skip" || msgText == "" {
				state.Step = StepNormal
				state.PickupSet = false
				state.DestSet = false
				state.CustomRequests = ""
				state.ItemDetail = ""
				state.GenderPref = ""
			} else {
				saveName := strings.TrimSpace(msgText)
				saveUserLocation(sender, saveName, state.PickupLat, state.PickupLng)
				sendMessage(client, senderJID, fmt.Sprintf("Location saved as *%s*!", saveName))
				state.Step = StepNormal
				state.PickupSet = false
				state.DestSet = false
				state.CustomRequests = ""
				state.ItemDetail = ""
				state.GenderPref = ""
			}

		case StepNormal:
			if state.IsDriver {
				if state.IsOnline {
					sendMessage(client, senderJID, "You are *READY* waiting for orders.\n`?unready` to go offline.")
				} else {
					sendMessage(client, senderJID, "You are *NOT READY*")
				}
			} else {
				sendMessage(client, senderJID, "Share your *pickup location* (WA share location) to order a ride, or type `!help` for more options.")
			}
		}
	}
}

func registerUser(phone string) string {
	url := fmt.Sprintf("%s/auth/register", apiGatewayURL)
	payload, _ := json.Marshal(map[string]string{"phone": phone, "password": "studex-" + phone, "name": phone})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("Error calling user-service: %v\n", err)
		return phone
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if resp.StatusCode == http.StatusCreated {
		if userObj, ok := result["user"].(map[string]interface{}); ok {
			if id, ok := userObj["id"].(string); ok && id != "" {
				fmt.Printf("[REGISTER] phone=%s user_id=%s\n", phone, id)
				return id
			}
			if id, ok := userObj["_id"].(string); ok && id != "" {
				fmt.Printf("[REGISTER] phone=%s user_id=%s\n", phone, id)
				return id
			}
		}
	}
	if resp.StatusCode == http.StatusConflict {
		return getUserID(phone)
	}
	return phone
}

func personalizeUser(phone, name, displayName, gender string) {
	url := fmt.Sprintf("%s/users/%s", apiGatewayURL, phone)
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
	} else {
		logErr("personalizeUser failed: phone=%s status=%d", phone, resp.StatusCode)
	}
}

func resolveUserID(phone string) string {
	resp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, phone))
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	defer resp.Body.Close()
	var user map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&user)
	if id, ok := user["id"].(string); ok && id != "" {
		return id
	}
	return ""
}

func getUserID(phone string) string {
	info := getUserInfo(phone)
	if info == nil {
		return phone
	}
	if id, ok := info["id"].(string); ok && id != "" {
		return id
	}
	if id, ok := info["_id"].(string); ok && id != "" {
		return id
	}
	return phone
}

func getUserInfo(phone string) map[string]interface{} {
	resp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, phone))
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}
	defer resp.Body.Close()
	var user map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&user)
	return user
}

func resolveCustomerRefID(phone string) string {
	if state, ok := loadUserState(phone); ok && state.UserID != "" && state.UserID != phone {
		return state.UserID
	}
	return getUserID(phone)
}

func getDriverInfo(phone string) (bool, bool) {
	url := fmt.Sprintf("%s/users/%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getDriverInfo HTTP failed: phone=%s err=%v", phone, err)
		return false, false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var user struct {
			Roles        []string `json:"roles"`
			DriverStatus string   `json:"driver_status"`
		}
		json.NewDecoder(resp.Body).Decode(&user)
		isDriver := false
		for _, r := range user.Roles {
			if r == "DRIVER" {
				isDriver = true
				break
			}
		}
		return isDriver, user.DriverStatus == "READY"
	}
	return false, false
}

func getDriverStatusField(phone string) string {
	url := fmt.Sprintf("%s/users/%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getDriverStatusField HTTP failed: phone=%s err=%v", phone, err)
		return ""
	}
	defer resp.Body.Close()
	var user struct {
		DriverStatus string `json:"driver_status"`
	}
	json.NewDecoder(resp.Body).Decode(&user)
	return user.DriverStatus
}

func updateDriverStatus(phone string, online bool) bool {
	status := "NOT_READY"
	if online {
		status = "READY"
	}
	payload, _ := json.Marshal(map[string]interface{}{"phone": phone, "status": status})
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/drivers/status", apiGatewayURL), bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	ok, _ := http.DefaultClient.Do(req)
	if ok != nil {
		ok.Body.Close()
	}
	if ok == nil || ok.StatusCode != http.StatusOK {
		logErr("updateDriverStatus failed: phone=%s online=%v", phone, online)
		return false
	}
	return true
}

func setGpsActive(refID string, active bool) bool {
	url := fmt.Sprintf("%s/location/%s/gps-status", apiGatewayURL, refID)
	payload, _ := json.Marshal(map[string]interface{}{"gps_active": active})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error setting GPS status for %s: %v\n", refID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func markDriverOnline(refID string) {
	fmt.Printf("[DRV-ONLINE] ref=%s\n", refID)
	url := fmt.Sprintf("%s/match/pool/%s/include?tag=readiness", apiGatewayURL, refID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error including candidate %s: %v\n", refID, err)
		return
	}
	resp.Body.Close()
	fmt.Printf("Candidate %s included (readiness filter cleared)\n", refID)
}

func addCandidateToPool(refID string, lat, lng float64) {
	fmt.Printf("[ADD-CANDIDATE] ref=%s lat=%.6f lng=%.6f\n", refID, lat, lng)
	url := fmt.Sprintf("%s/match/pool", apiGatewayURL)
	payload, _ := json.Marshal(map[string]interface{}{
		"ref_id": refID,
		"attributes": map[string]interface{}{
			"latitude":  lat,
			"longitude": lng,
		},
	})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("Error adding candidate %s to pool: %v\n", refID, err)
		return
	}
	resp.Body.Close()
	fmt.Printf("Candidate %s added to matching pool\n", refID)
}

func markDriverOffline(refID string) {
	fmt.Printf("[DRV-OFFLINE] ref=%s\n", refID)
	url := fmt.Sprintf("%s/match/pool/%s/exclude?tag=readiness", apiGatewayURL, refID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("Error marking candidate offline %s: %v\n", refID, err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Candidate %s excluded (readiness filter set)\n", refID)
}

func getDriverUserID(refID string) string {
	resp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, refID))
	if err != nil || resp.StatusCode != http.StatusOK {
		logErr("getDriverUserID HTTP failed: ref=%s err=%v", refID, err)
		return ""
	}
	defer resp.Body.Close()
	var d struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp.Body).Decode(&d)
	return d.ID
}

func syncOfflineDrivers() {
	resp, err := http.Get(fmt.Sprintf("%s/location", apiGatewayURL))
	if err != nil {
		fmt.Printf("syncOfflineDrivers: failed to fetch locations: %v\n", err)
		return
	}
	defer resp.Body.Close()
	var locs []struct {
		RefID string `json:"ref_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&locs); err != nil {
		logErr("syncOfflineDrivers decode failed: %v", err)
		return
	}
	count := 0
	for _, loc := range locs {
		if loc.RefID == "" {
			continue
		}
		phone := resolvePhone(loc.RefID)
		isOffline := false
		if state, ok := loadUserState(phone); ok {
			isOffline = !state.IsOnline
		} else {
			drvResp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, loc.RefID))
			if err == nil && drvResp.StatusCode == http.StatusOK {
				var drv struct {
					DriverStatus string `json:"driver_status"`
				}
				json.NewDecoder(drvResp.Body).Decode(&drv)
				drvResp.Body.Close()
				isOffline = drv.DriverStatus == "NOT_READY" || drv.DriverStatus == ""
			}
		}
		if isOffline {
				url := fmt.Sprintf("%s/match/pool/%s/exclude?tag=readiness", apiGatewayURL, loc.RefID)
				req, _ := http.NewRequest(http.MethodPut, url, nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					logErr("syncOfflineDrivers exclude HTTP failed: ref=%s err=%v", loc.RefID, err)
				}
				if resp != nil {
					resp.Body.Close()
					count++
				}
			} else {
				url := fmt.Sprintf("%s/match/pool/%s/include?tag=readiness", apiGatewayURL, loc.RefID)
				req, _ := http.NewRequest(http.MethodPut, url, nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					logErr("syncOfflineDrivers include HTTP failed: ref=%s err=%v", loc.RefID, err)
				}
				if resp != nil {
					resp.Body.Close()
					count++
				}
			}
	}
	fmt.Printf("syncOfflineDrivers: synced %d candidates\n", count)
}

func registerDriverLocation(refID string, lat, lng float64) {
	url := fmt.Sprintf("%s/location/", apiGatewayURL)
	payload, _ := json.Marshal(map[string]interface{}{
		"ref_id":    refID,
		"latitude":  lat,
		"longitude": lng,
	})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("Error registering driver location: %v\n", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Driver %s location registered\n", refID)
}

type driverCheckResult struct {
	gpsActive bool
	location  map[string]interface{}
	refID     string
}

func checkDriverGpsAndLocation(phone string) driverCheckResult {
	driverInfo, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, phone))
	if err != nil || driverInfo.StatusCode != http.StatusOK {
		return driverCheckResult{}
	}
	defer driverInfo.Body.Close()
	var driver map[string]interface{}
	json.NewDecoder(driverInfo.Body).Decode(&driver)
	driverID, _ := driver["id"].(string)
	if driverID == "" {
		return driverCheckResult{}
	}

	var result driverCheckResult
	result.refID = driverID

	resp, err := http.Get(fmt.Sprintf("%s/location/%s/gps-status", apiGatewayURL, driverID))
	if err == nil && resp.StatusCode == http.StatusOK {
		var gs map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&gs)
		result.gpsActive, _ = gs["gps_active"].(bool)
	}
	resp.Body.Close()

	locResp, err := http.Get(fmt.Sprintf("%s/location/%s", apiGatewayURL, driverID))
	if err == nil && locResp.StatusCode == http.StatusOK {
		var loc map[string]interface{}
		json.NewDecoder(locResp.Body).Decode(&loc)
		if _, hasLat := loc["latitude"]; hasLat {
			result.location = loc
		}
	}
	locResp.Body.Close()

	return result
}

type priceResponse struct {
	Total      float64 `json:"total"`
	DistanceKm float64 `json:"distance_km"`
}

func getPriceEstimate(pickupLat, pickupLng, destLat, destLng float64, serviceType string) priceResponse {
	url := fmt.Sprintf("%s/rides/price?origin_lat=%f&origin_lng=%f&dest_lat=%f&dest_lng=%f&service_type=%s", apiGatewayURL, pickupLat, pickupLng, destLat, destLng, serviceType)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getPriceEstimate HTTP failed: err=%v", err)
		return priceResponse{}
	}
	defer resp.Body.Close()
	var result priceResponse
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

func requestRide(customerPhone string, pickupLat, pickupLng, destLat, destLng float64, serviceType string) map[string]interface{} {
	custRefID := resolveCustomerRefID(customerPhone)
	url := fmt.Sprintf("%s/rides/request", apiGatewayURL)
	payload, _ := json.Marshal(map[string]interface{}{
		"customer_ref_id": custRefID,
		"pickup_lat":      pickupLat,
		"pickup_lng":      pickupLng,
		"dest_lat":        destLat,
		"dest_lng":        destLng,
		"service_type":    serviceType,
	})
	fmt.Printf("[RIDE-REQ] customer=%s pickup=%.4f,%.4f dest=%.4f,%.4f svc=%s\n", customerPhone, pickupLat, pickupLng, destLat, destLng, serviceType)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("[RIDE-REQ-FAIL] err=%v\n", err)
		return nil
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("[RIDE-REQ-RESP] status=%d result=%v\n", resp.StatusCode, result)
	return result
}

func getDriverActiveTrip(driverPhone string) map[string]interface{} {
	candidates := []string{driverPhone}
	resp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, driverPhone))
	if err == nil && resp.StatusCode == http.StatusOK {
		var d map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&d)
		resp.Body.Close()
		if id, ok := d["id"].(string); ok && id != driverPhone {
			candidates = append(candidates, id)
		}
	}
	for _, refID := range candidates {
		url := fmt.Sprintf("%s/trips/driver/%s", apiGatewayURL, refID)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		var trips []map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&trips)
		resp.Body.Close()
		for _, t := range trips {
			s, _ := t["status"].(string)
			if s == "pending_acceptance" || s == "bargaining" || s == "deal" || s == "accepted" || s == "in_progress" {
				return t
			}
		}
	}
	return nil
}

func notifyWABA(phone, eventType string, price float64, orderID string) {
	if wabaURL == "" {
		return
	}
	body, _ := json.Marshal(map[string]interface{}{
		"phone": phone,
		"type":  eventType,
		"price": price,
		"order": orderID,
	})
	http.Post(wabaURL+"/api/notify", "application/json", bytes.NewBuffer(body))
}

func acceptTrip(tripID string) bool {
	url := fmt.Sprintf("%s/trips/%s/accept", apiGatewayURL, tripID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("acceptTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func startTrip(tripID string) bool {
	url := fmt.Sprintf("%s/trips/%s/start", apiGatewayURL, tripID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("startTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func completeTrip(tripID string, paymentStatus string) bool {
	url := fmt.Sprintf("%s/trips/%s/complete", apiGatewayURL, tripID)
	payload, _ := json.Marshal(map[string]interface{}{"payment_status": paymentStatus})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("completeTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func completeTripWithDebt(tripID string, debtAmount float64) bool {
	url := fmt.Sprintf("%s/trips/%s/complete", apiGatewayURL, tripID)
	payload, _ := json.Marshal(map[string]interface{}{"payment_status": "debt", "debt_amount": debtAmount})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("completeTripWithDebt HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func rejectTrip(tripID string, reason string) bool {
	url := fmt.Sprintf("%s/trips/%s/reject", apiGatewayURL, tripID)
	payload, _ := json.Marshal(map[string]interface{}{"reason": reason})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("rejectTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func rejectMatchRequest(orderID, driverRefID string) {
	url := fmt.Sprintf("%s/match/requests/order/%s/reject", apiGatewayURL, orderID)
	payload, _ := json.Marshal(map[string]interface{}{"excluded_driver": driverRefID})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("rejectMatchRequest HTTP failed: order=%s err=%v", orderID, err)
		return
	}
	resp.Body.Close()
	fmt.Printf("[MATCH-REJECT-API] order=%s excluded_driver=%s status=%d\n", orderID, driverRefID, resp.StatusCode)
}

func cancelMatchRequest(orderID string) {
	if orderID == "" {
		return
	}
	url := fmt.Sprintf("%s/match/requests/order/%s", apiGatewayURL, orderID)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("cancelMatchRequest HTTP failed: order=%s err=%v", orderID, err)
		return
	}
	resp.Body.Close()
	fmt.Printf("[MATCH-CANCEL] order=%s status=%d\n", orderID, resp.StatusCode)
}

func cancelTrip(tripID, reason, cancelledBy, phone string) bool {
	url := fmt.Sprintf("%s/trips/%s/cancel", apiGatewayURL, tripID)
	payload, _ := json.Marshal(map[string]interface{}{"reason": reason, "cancelled_by": cancelledBy, "phone": phone})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("cancelTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func abortTrip(tripID, reason, phone string) bool {
	url := fmt.Sprintf("%s/trips/%s/abort", apiGatewayURL, tripID)
	payload, _ := json.Marshal(map[string]interface{}{"reason": reason, "aborted_by": "driver", "phone": phone})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("abortTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func bidTrip(tripID, bidderPhone, bidderRole string, amount float64, reason string) bool {
	url := fmt.Sprintf("%s/trips/%s/bid", apiGatewayURL, tripID)
	payload, _ := json.Marshal(map[string]interface{}{
		"amount":      amount,
		"reason":      reason,
		"bidder_role": bidderRole,
	})
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("bidTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func dealTrip(tripID string) bool {
	url := fmt.Sprintf("%s/trips/%s/deal", apiGatewayURL, tripID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("dealTrip HTTP failed: trip=%s err=%v", tripID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func getCustomerActiveTrip(customerPhone string) map[string]interface{} {
	candidates := []string{customerPhone}
	userInfo := getUserInfo(customerPhone)
	if userInfo != nil {
		if id, ok := userInfo["id"].(string); ok && id != "" && id != customerPhone {
			candidates = append(candidates, id)
		}
	}
	for _, refID := range candidates {
		url := fmt.Sprintf("%s/trips/customer/%s", apiGatewayURL, refID)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		var trips []map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&trips)
		resp.Body.Close()
		for _, t := range trips {
			s, _ := t["status"].(string)
			if s == "pending_acceptance" || s == "bargaining" || s == "deal" || s == "accepted" || s == "in_progress" {
				return t
			}
		}
	}
	return nil
}

func serviceName(svcType string) string {
	switch svcType {
	case "anjem":
		return "Anjem (escort ride)"
	case "jastip":
		return "Jastip (delivery)"
	default:
		return svcType
	}
}

func requestRideWithDetails(customerPhone string, pickupLat, pickupLng, destLat, destLng float64, serviceType, customRequests, itemDetail, genderPref string) map[string]interface{} {
	custRefID := resolveCustomerRefID(customerPhone)
	url := fmt.Sprintf("%s/rides/request", apiGatewayURL)
	payload := map[string]interface{}{
		"customer_ref_id": custRefID,
		"pickup_lat":      pickupLat,
		"pickup_lng":      pickupLng,
		"dest_lat":        destLat,
		"dest_lng":        destLng,
		"service_type":    serviceType,
	}
	if customRequests != "" {
		payload["custom_requests"] = customRequests
	}
	if itemDetail != "" {
		payload["item_detail"] = itemDetail
	}
	if genderPref != "" {
		payload["gender_pref"] = genderPref
	}
	data, _ := json.Marshal(payload)
	fmt.Printf("[RIDE-REQ] customer=%s pickup=%.4f,%.4f dest=%.4f,%.4f svc=%s custom=%q\n", customerPhone, pickupLat, pickupLng, destLat, destLng, serviceType, customRequests)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		fmt.Printf("[RIDE-REQ-FAIL] err=%v\n", err)
		return nil
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("[RIDE-REQ-RESP] status=%d result=%v\n", resp.StatusCode, result)
	return result
}

// mintCustomerToken logs the customer in with the deterministic password
// registerUser() sets on signup, to get a short-lived JWT for the trip web
// app link. Never exposed to the user directly.
func mintCustomerToken(phone string) string {
	url := fmt.Sprintf("%s/auth/login", apiGatewayURL)
	payload, _ := json.Marshal(map[string]string{"phone": phone, "password": "studex-" + phone})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		logErr("mintCustomerToken HTTP failed: phone=%s err=%v", phone, err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logErr("mintCustomerToken failed: phone=%s status=%d", phone, resp.StatusCode)
		return ""
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	tok, _ := result["token"].(string)
	return tok
}

// buildTripLink returns the temporary web app link for bidding, chat and
// driver-location tracking for a just-requested ride, or "" if a token
// couldn't be minted (e.g. WEB_APP_BASE_URL not configured). Pickup/dest/
// service are embedded so the page can self-serve a retry if matching
// times out with no driver found, without depending on matching-service's
// transient request data still being around.
func buildTripLink(phone, orderID string, pickupLat, pickupLng, destLat, destLng float64, serviceType string) string {
	if webAppBaseURL == "" {
		return ""
	}
	tok := mintCustomerToken(phone)
	if tok == "" {
		return ""
	}
	return fmt.Sprintf("%s?oid=%s&tok=%s&plat=%f&plng=%f&dlat=%f&dlng=%f&svc=%s",
		webAppBaseURL, orderID, tok, pickupLat, pickupLng, destLat, destLng, serviceType)
}

func hasActiveDebt(phone string) bool {
	url := fmt.Sprintf("%s/drivers/debts/active?phone=%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		logErr("hasActiveDebt HTTP failed: phone=%s err=%v", phone, err)
		return false
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if has, ok := result["has_active"].(bool); ok {
		return has
	}
	return false
}

func submitRating(sender, raterType, rateeID, tripID, orderID string, score int, review string) bool {
	url := fmt.Sprintf("%s/ratings", apiGatewayURL)
	payload, _ := json.Marshal(map[string]interface{}{
		"rater_type": raterType,
		"rater_id":   sender,
		"ratee_id":   rateeID,
		"trip_id":    tripID,
		"order_id":   orderID,
		"score":      score,
		"review":     review,
	})
	log.Printf("[RATING] posting to %s payload=%s", url, string(payload))
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		logErr("submitRating HTTP failed: rater=%s err=%v", sender, err)
		return false
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	log.Printf("[RATING] response status=%d body=%s", resp.StatusCode, string(bodyBytes))
	return resp.StatusCode == http.StatusCreated
}

func getDriverRecentTrips(phone string, limit int) ([]map[string]interface{}, error) {
	candidates := []string{phone}
	userInfo := getUserInfo(phone)
	if userInfo != nil {
		if id, ok := userInfo["id"].(string); ok && id != "" && id != phone {
			candidates = append(candidates, id)
		}
	}
	for _, refID := range candidates {
		url := fmt.Sprintf("%s/trips/driver/%s", apiGatewayURL, refID)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		var trips []map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&trips)
		resp.Body.Close()
		if len(trips) > 0 {
			if limit > 0 && len(trips) > limit {
				trips = trips[:limit]
			}
			return trips, nil
		}
	}
	return nil, nil
}

func getCustomerRecentTrips(phone string, limit int) ([]map[string]interface{}, error) {
	candidates := []string{phone}
	userInfo := getUserInfo(phone)
	if userInfo != nil {
		if id, ok := userInfo["id"].(string); ok && id != "" && id != phone {
			candidates = append(candidates, id)
		}
	}
	for _, refID := range candidates {
		url := fmt.Sprintf("%s/trips/customer/%s", apiGatewayURL, refID)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		var trips []map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&trips)
		resp.Body.Close()
		if len(trips) > 0 {
			if limit > 0 && len(trips) > limit {
				trips = trips[:limit]
			}
			return trips, nil
		}
	}
	return nil, nil
}

func getReputationScore(userID string) float64 {
	url := fmt.Sprintf("%s/reputation/%s", apiGatewayURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getReputationScore HTTP failed: user=%s err=%v", userID, err)
		return 0
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if score, ok := result["score"].(float64); ok {
		return score
	}
	return 0
}

func getReputation(userID string, limit int) []map[string]interface{} {
	url := fmt.Sprintf("%s/ratings/%s", apiGatewayURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getReputation HTTP failed: user=%s err=%v", userID, err)
		return nil
	}
	defer resp.Body.Close()
	var ratings []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&ratings)
	if limit > 0 && len(ratings) > limit {
		ratings = ratings[:limit]
	}
	return ratings
}

func getDriverDebts(phone string) []map[string]interface{} {
	url := fmt.Sprintf("%s/drivers/debts?phone=%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getDriverDebts HTTP failed: phone=%s err=%v", phone, err)
		return nil
	}
	defer resp.Body.Close()
	var debts []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&debts)
	return debts
}

func getCustomerDebts(phone string) []map[string]interface{} {
	url := fmt.Sprintf("%s/trips?customer_ref_id=%s&payment_status=debt", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getCustomerDebts HTTP failed: phone=%s err=%v", phone, err)
		return nil
	}
	defer resp.Body.Close()
	var trips []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&trips)
	return trips
}

func confirmDebt(debtID string) bool {
	url := fmt.Sprintf("%s/drivers/debts/%s/pay", apiGatewayURL, debtID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("confirmDebt HTTP failed: debt=%s err=%v", debtID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func denyDebt(debtID string) bool {
	url := fmt.Sprintf("%s/drivers/debts/%s/dispute", apiGatewayURL, debtID)
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logErr("denyDebt HTTP failed: debt=%s err=%v", debtID, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func confirmCustomerPayment(phone string) {
	url := fmt.Sprintf("%s/drivers/debts/active?phone=%s", apiGatewayURL, phone)
	resp, err := http.Get(url)
	if err != nil {
		logErr("confirmCustomerPayment HTTP failed: phone=%s err=%v", phone, err)
		return
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("Customer %s confirmed payment\n", phone)
}

func submitLeaveRequest(phone, leaveType, reason string) bool {
	url := fmt.Sprintf("%s/drivers/leave-requests", apiGatewayURL)
	payload, _ := json.Marshal(map[string]interface{}{
		"phone":  phone,
		"type":   leaveType,
		"reason": reason,
	})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		logErr("submitLeaveRequest HTTP failed: phone=%s err=%v", phone, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK
}

func saveUserLocation(phone, name string, lat, lng float64) bool {
	url := fmt.Sprintf("%s/users/%s/saved-locations", apiGatewayURL, phone)
	payload, _ := json.Marshal(map[string]interface{}{
		"name": name,
		"lat":  lat,
		"lng":  lng,
	})
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		logErr("saveUserLocation HTTP failed: phone=%s err=%v", phone, err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated
}

func fetchDriverProfile(refID string) map[string]interface{} {
	url := fmt.Sprintf("%s/users/%s", apiGatewayURL, refID)
	resp, err := http.Get(url)
	if err != nil {
		logErr("fetchDriverProfile HTTP failed: ref=%s err=%v", refID, err)
		return nil
	}
	defer resp.Body.Close()
	var driver map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&driver)
	return driver
}

func getPendingRatings(userID string) []map[string]interface{} {
	url := fmt.Sprintf("%s/pending-ratings/%s", apiGatewayURL, userID)
	resp, err := http.Get(url)
	if err != nil {
		logErr("getPendingRatings HTTP failed: user=%s err=%v", userID, err)
		return nil
	}
	defer resp.Body.Close()
	var pending []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pending)
	return pending
}

var chatMongoDB *mongo.Database

func initChatMongo() {
	uri := config.GetEnv("MONGODB_URI", "")
	dbName := config.GetEnv("CHAT_DB_NAME", "studex-chat")
	if uri == "" {
		fmt.Println("Warning: MONGODB_URI not set, chat persistence disabled")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		fmt.Printf("Warning: chat MongoDB connect failed: %v\n", err)
		return
	}
	if err := client.Ping(ctx, nil); err != nil {
		fmt.Printf("Warning: chat MongoDB ping failed: %v\n", err)
		return
	}
	chatMongoDB = client.Database(dbName)
	_, err = chatMongoDB.Collection("chat_sessions").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "trip_id", Value: 1}}},
		{Keys: bson.D{{Key: "order_id", Value: 1}}},
	})
	if err != nil {
		logErr("chat_sessions index creation failed: %v", err)
	}
	_, err = chatMongoDB.Collection("chat_messages").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "session_id", Value: 1}}},
		{Keys: bson.D{{Key: "created_at", Value: 1}}},
	})
	if err != nil {
		logErr("chat_messages index creation failed: %v", err)
	}
	_, err = chatMongoDB.Collection("wa_chat_log").Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "created_at", Value: -1}}},
		{Keys: bson.D{{Key: "phone", Value: 1}}},
	})
	if err != nil {
		logErr("wa_chat_log index creation failed: %v", err)
	}
	fmt.Printf("Chat MongoDB connected: %s\n", dbName)
}

func saveChatSession(tripID, orderID, driverPhone, customerPhone string) string {
	if chatMongoDB == nil {
		return ""
	}
	session := bson.M{
		"trip_id":        tripID,
		"order_id":       orderID,
		"driver_phone":   driverPhone,
		"customer_phone": customerPhone,
		"started_at":     time.Now(),
	}
	result, err := chatMongoDB.Collection("chat_sessions").InsertOne(context.Background(), session)
	if err != nil {
		fmt.Printf("Error saving chat session: %v\n", err)
		return ""
	}
	return result.InsertedID.(primitive.ObjectID).Hex()
}

func closeChatSession(tripID string) {
	if chatMongoDB == nil {
		return
	}
	_, err := chatMongoDB.Collection("chat_sessions").UpdateOne(
		context.Background(),
		bson.M{"trip_id": tripID},
		bson.M{"$set": bson.M{"ended_at": time.Now()}},
	)
	if err != nil {
		logErr("closeChatSession failed: trip=%s err=%v", tripID, err)
	}
}

func saveChatMessage(phone, senderType, msgType, message string) {
	if chatMongoDB == nil {
		return
	}
	state, ok := loadUserState(phone)
	if !ok || state == nil || state.ChatSessionID == "" {
		return
	}
	sessionOID, err := primitive.ObjectIDFromHex(state.ChatSessionID)
	if err != nil {
		logErr("saveChatMessage invalid session ID: phone=%s session=%s err=%v", phone, state.ChatSessionID, err)
		return
	}
	_, err = chatMongoDB.Collection("chat_messages").InsertOne(context.Background(), bson.M{
		"session_id":   sessionOID,
		"sender_type":  senderType,
		"sender_phone": phone,
		"message_type": msgType,
		"message":      message,
		"created_at":   time.Now(),
	})
	if err != nil {
		logErr("saveChatMessage insert failed: phone=%s err=%v", phone, err)
	}
}

func logWAChat(direction, phone, role, message string) {
	if chatMongoDB == nil {
		return
	}
	_, _ = chatMongoDB.Collection("wa_chat_log").InsertOne(context.Background(), bson.M{
		"direction":  direction,
		"phone":      phone,
		"role":       role,
		"message":    message,
		"created_at": time.Now(),
	})
}

func waJID(phone string) types.JID {
	return types.JID{
		User:   phone,
		Server: types.DefaultUserServer,
	}
}

func startRedisEventRelay(ctx context.Context, rdb *redis.Client, client *whatsmeow.Client) {
	matchSub, err := events.Subscribe(ctx, rdb, events.RedisChannelMatchEvents)
	if err != nil {
		fmt.Printf("Redis match subscribe failed: %v\n", err)
		return
	}
	tripSub, err := events.Subscribe(ctx, rdb, events.RedisChannelTripEvents)
	if err != nil {
		fmt.Printf("Redis trip subscribe failed: %v\n", err)
		return
	}
	fmt.Println("Redis event relay started")

	for {
		select {
		case <-ctx.Done():
			matchSub.Close()
			tripSub.Close()
			return
		case msg, ok := <-matchSub.Ch:
			if !ok {
				return
			}
			var evt events.Event
			if json.Unmarshal([]byte(msg.Payload), &evt) != nil {
				continue
			}
			handleRelayEvent(client, evt)
		case msg, ok := <-tripSub.Ch:
			if !ok {
				return
			}
			var evt events.Event
			if json.Unmarshal([]byte(msg.Payload), &evt) != nil {
				continue
			}
			handleRelayEvent(client, evt)
		}
	}
}

func startDriverStatusRelay(ctx context.Context, rdb *redis.Client, client *whatsmeow.Client) {
	sub, err := events.Subscribe(ctx, rdb, events.RedisChannelDriverStatus)
	if err != nil {
		fmt.Printf("Redis driver-status subscribe failed: %v\n", err)
		return
	}
	fmt.Println("Driver status relay started")
	for {
		select {
		case <-ctx.Done():
			sub.Close()
			return
		case msg, ok := <-sub.Ch:
			if !ok {
				return
			}
			var data events.DriverStatusChangedData
			if events.ParseEvent([]byte(msg.Payload), &data) != nil {
				continue
			}
			phone := data.Phone
			if phone == "" {
				phone = resolvePhone(data.DriverRefID)
			}
			if !strings.HasPrefix(phone, "62") {
				continue
			}
			if st, ok := loadUserState(phone); ok {
				if st.IsOnline != data.IsOnline {
					st.IsOnline = data.IsOnline
					saveUserState(phone, st)
					fmt.Printf("[DRV-STATUS] %s → online=%v (real-time sync)\n", phone, data.IsOnline)
					if data.IsOnline {
						sendMessage(client, waJID(phone), "You are now *READY* ✅\nWaiting for orders...\nType `?unready` to stop receiving orders.")
					} else {
						sendMessage(client, waJID(phone), "You are now *NOT READY* ❌\nYou can also order a ride by sharing a pickup location.")
					}
				}
			}
		}
	}
}

func startGpsStatusRelay(ctx context.Context, rdb *redis.Client, client *whatsmeow.Client) {
	sub, err := events.Subscribe(ctx, rdb, events.RedisChannelGpsStatus)
	if err != nil {
		fmt.Printf("Redis GPS subscribe failed: %v\n", err)
		return
	}
	fmt.Println("GPS status relay started")
	gpsRelayActive.Store(1)
	defer gpsRelayActive.Store(0)
	for {
		select {
		case <-ctx.Done():
			sub.Close()
			return
		case msg, ok := <-sub.Ch:
			if !ok {
				return
			}
			var data events.GpsStatusData
			if json.Unmarshal([]byte(msg.Payload), &data) != nil {
				continue
			}
			phone := resolvePhone(data.RefID)
			if !strings.HasPrefix(phone, "62") {
				continue
			}
			if !data.GpsActive {
				if state, ok := loadUserState(phone); ok && !state.IsOnline {
					continue
				}
				markDriverOffline(data.RefID)
				if uid := getDriverUserID(data.RefID); uid != "" && uid != data.RefID {
					markDriverOffline(uid)
				}
				updateDriverStatus(phone, false)
		if state, ok := loadUserState(phone); ok {
					state.IsOnline = false
					saveUserState(phone, state)
				}
				sendMessage(client, waJID(phone), "GPS turned off — you are now *NOT READY*\nTurn on GPS and use `?ready` to go online.")
				fmt.Printf("[GPS OFF] %s → auto-unready\n", phone)
			} else {
				if state, ok := loadUserState(phone); ok && !state.IsOnline {
					sendMessage(client, waJID(phone), "GPS is on. Use `?ready` to go online.")
				}
			}
		}
	}
}

func resolvePhone(refID string) string {
	if strings.HasPrefix(refID, "62") {
		return refID
	}
	resp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, refID))
	if err == nil && resp.StatusCode == http.StatusOK {
		var user map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&user)
		resp.Body.Close()
		if phones, ok := user["phone"].([]interface{}); ok && len(phones) > 0 {
			if p, ok := phones[0].(string); ok {
				return p
			}
		}
		if p, ok := user["phone"].(string); ok && p != "" {
			return p
		}
	}
	drvResp, err := http.Get(fmt.Sprintf("%s/users/%s", apiGatewayURL, refID))
	if err == nil && drvResp.StatusCode == http.StatusOK {
		var user map[string]interface{}
		json.NewDecoder(drvResp.Body).Decode(&user)
		drvResp.Body.Close()
		if phones, ok := user["phone"].([]interface{}); ok && len(phones) > 0 {
			if p, ok := phones[0].(string); ok && p != "" {
				return p
			}
		}
	}
	return refID
}

func handleRelayEvent(client *whatsmeow.Client, evt events.Event) {
	dataBytes, _ := json.Marshal(evt.Data)
	switch evt.Type {
	case events.EventMatchCompleted:
		var data events.MatchCompletedData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		driverPhone := resolvePhone(data.DriverRefID)
		customerPhone := resolvePhone(data.CustomerRefID)
		fmt.Printf("[MATCH-EVENT] driver=%s customer=%s order=%s score=%.2f\n", driverPhone, customerPhone, data.OrderID, data.Score)
		if !strings.HasPrefix(driverPhone, "62") || !strings.HasPrefix(customerPhone, "62") {
			fmt.Printf("[MATCH-EVENT-SKIP] driver=%s customer=%s — not valid phones\n", driverPhone, customerPhone)
			return
		}
		estimatedPrice := data.EstimatedPrice
		svcType := data.ServiceType
		svcLabel := serviceName(svcType)
		if svcLabel == "" {
			svcLabel = "Ride"
		}
		driverMsg := fmt.Sprintf("New trip request!\n%s\nOrder: %s\nCustomer: %s\nPickup: %.4f, %.4f\nScore: %.2f", svcLabel, data.OrderID, customerPhone, data.PickupLat, data.PickupLng, data.Score)
		custRep := getReputationScore(customerPhone)
		if custRep > 0 {
			driverMsg += fmt.Sprintf("\nCustomer rep: %.1f/5", custRep)
		}
		if estimatedPrice > 0 {
			driverMsg += fmt.Sprintf("\nPrice: Rp %.0f", estimatedPrice)
			if drvState, ok := loadUserState(driverPhone); ok && drvState != nil {
				drvState.CurrentBidPrice = estimatedPrice
				drvState.ActiveTrip = data.OrderID
				drvState.MatchedCustomerPhone = customerPhone
				drvState.MenuContext = "match"
				drvState.LastBidder = ""
				saveUserState(driverPhone, drvState)
			}
		}
		driverMsg += "\n\nq: Accept\nw: Reject (with reason)\n\n⏱ 1 min to respond"
		sendMessage(client, waJID(driverPhone), driverMsg)
		sendLocation(client, waJID(driverPhone), data.PickupLat, data.PickupLng, "Pickup point")
		custMsg := fmt.Sprintf("Driver found! Waiting for driver to accept...\nOrder: %s", data.OrderID)
		if estimatedPrice > 0 {
			drvInfo := fetchDriverProfile(data.DriverRefID)
			custMsg = fmt.Sprintf("Driver found!\n%s — Rp %.0f", svcLabel, estimatedPrice)
			if drvInfo != nil {
				if name, ok := drvInfo["fullname"].(string); ok && name != "" {
					custMsg += fmt.Sprintf("\nDriver: %s", name)
				}
				if plate, ok := drvInfo["vehicle_info"].(map[string]interface{}); ok {
					if lp, ok := plate["license_plate"].(string); ok && lp != "" {
						custMsg += fmt.Sprintf("\nPlate: %s", lp)
					}
					if vt, ok := plate["vehicle_type"].(string); ok && vt != "" {
						custMsg += fmt.Sprintf("\nVehicle: %s", vt)
					}
				}
			}
			drvRep := getReputationScore(data.DriverRefID)
			if drvRep > 0 {
				custMsg += fmt.Sprintf("\nRating: %.1f/5", drvRep)
			}
			custMsg += "\nWaiting for driver response..."
			if custState, ok := loadUserState(customerPhone); ok && custState != nil {
				custState.CurrentBidPrice = estimatedPrice
				custState.ActiveTrip = data.OrderID
				custState.LastBidder = ""
				saveUserState(customerPhone, custState)
			}
		}
		notifyWABA(customerPhone, "driver_found", estimatedPrice, data.OrderID)

		go func() {
			orderID := data.OrderID
			drvPhone := driverPhone
			custPhone := customerPhone
			time.Sleep(30 * time.Second)
			if drvState, ok := loadUserState(drvPhone); ok && drvState != nil && drvState.ActiveTrip == orderID {
				sendMessage(client, waJID(drvPhone), "⏳ 30s remaining — please respond!")
			}
			time.Sleep(30 * time.Second)
			drvState, ok := loadUserState(drvPhone)
			if !ok || drvState == nil || drvState.ActiveTrip != orderID {
				return
			}
			fmt.Printf("[MATCH-TIMEOUT] driver=%s order=%s — ignored, auto-rejecting\n", drvPhone, orderID)
			drvState.ActiveTrip = ""
			drvState.CurrentBidPrice = 0
			drvState.LastBidder = ""
			drvState.MenuContext = ""
			drvState.MatchedCustomerPhone = ""
			saveUserState(drvPhone, drvState)
			rejectMatchRequest(orderID, checkDriverGpsAndLocation(drvPhone).refID)
			check := checkDriverGpsAndLocation(drvPhone)
			if check.refID != "" {
				markDriverOffline(check.refID)
				time.Sleep(300 * time.Millisecond)
				markDriverOnline(check.refID)
				lat, lng := 0.0, 0.0
				if check.gpsActive && check.location != nil {
					lat, _ = check.location["latitude"].(float64)
					lng, _ = check.location["longitude"].(float64)
				}
				addCandidateToPool(check.refID, lat, lng)
			}
			sendMessage(client, waJID(drvPhone), "No response (1 min) — match auto-rejected. Waiting for next trip...")
			sendMessage(client, waJID(custPhone), "Driver did not respond. Searching for another driver...\nPlease wait.")
		}()

	case events.EventTripCreated:
		var data events.TripCreatedData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}

	case events.EventTripBargaining:
		var data events.TripBargainingData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		driverPhone := resolvePhone(data.DriverRefID)
		customerPhone := resolvePhone(data.CustomerRefID)
		if strings.HasPrefix(driverPhone, "62") && data.LastBidder == "customer" {
			sendMessage(client, waJID(driverPhone), fmt.Sprintf("Customer bids Rp %.0f\nReason: -\n\n?deal — Accept current price\n?bid <amount> <reason> — Counter bid", data.CurrentBidPrice))
		}
		if strings.HasPrefix(customerPhone, "62") && data.LastBidder == "driver" {
			sendMessage(client, waJID(customerPhone), fmt.Sprintf("Driver bids Rp %.0f\nReason: -\n\n?deal — Accept current price\n?bid <amount> <reason> — Counter bid", data.CurrentBidPrice))
		}

	case events.EventTripStarted:
		var data events.TripStartedData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		driverPhone := resolvePhone(data.DriverRefID)
		customerPhone := resolvePhone(data.CustomerRefID)
		if strings.HasPrefix(driverPhone, "62") {
			sendMessage(client, waJID(customerPhone), "Trip started! Your driver is on the way.")
		}

	case events.EventTripCompleted:
		var data events.TripCompletedData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		driverPhone := resolvePhone(data.DriverRefID)
		customerPhone := resolvePhone(data.CustomerRefID)
		if strings.HasPrefix(driverPhone, "62") {
			if drvState, ok := loadUserState(driverPhone); ok && drvState != nil {
				drvState.ActiveTrip = ""
				drvState.ChatSessionID = ""
				drvState.CurrentBidPrice = 0
				drvState.LastBidder = ""
				drvState.MenuContext = "rate"
				drvState.PendingAction = "auto_ready_after_rate"
				saveUserState(driverPhone, drvState)
			}
			if data.PaymentStatus == "debt" {
				debtMsg := fmt.Sprintf("Trip completed! *You have debt.*\nTotal: Rp %.0f\nDebt: Rp %.0f\n\nq: Pay\nw: Confirm paid\ne: Rate driver (e 1-5)", data.FinalPrice, data.DebtAmount)
				sendMessage(client, waJID(customerPhone), debtMsg)
				sendMessage(client, waJID(driverPhone), fmt.Sprintf("Trip completed! (Debt) Earnings: Rp %.0f\n\nq: Rate (q 1-5)\nw: Skip", data.FinalPrice))
				if custState, ok := loadUserState(customerPhone); ok && custState != nil {
					custState.MenuContext = "debt"
					custState.ActiveTrip = ""
					custState.ChatSessionID = ""
					custState.CurrentBidPrice = 0
					custState.LastBidder = ""
					custState.HasDebt = true
					custState.DebtAmount = data.DebtAmount
					saveUserState(customerPhone, custState)
				}
			} else {
				sendMessage(client, waJID(customerPhone), fmt.Sprintf("Trip completed! Thank you for riding with StudEx.\nTotal: Rp %.0f\n\nq: Rate (q 1-5)\nw: Skip", data.FinalPrice))
				sendMessage(client, waJID(driverPhone), fmt.Sprintf("Trip completed! (Paid) Earnings: Rp %.0f\n\nq: Rate (q 1-5)\nw: Skip", data.FinalPrice))
				if custState, ok := loadUserState(customerPhone); ok && custState != nil {
					custState.MenuContext = "rate"
					custState.ActiveTrip = ""
					custState.ChatSessionID = ""
					custState.CurrentBidPrice = 0
					custState.LastBidder = ""
					saveUserState(customerPhone, custState)
				}
			}
			deletePartner(driverPhone)
			deletePartner(customerPhone)
			go func() {
				time.Sleep(2 * time.Second)
				check := checkDriverGpsAndLocation(driverPhone)
				if check.refID != "" {
					markDriverOnline(check.refID)
					lat, lng := 0.0, 0.0
					if check.gpsActive && check.location != nil {
						lat, _ = check.location["latitude"].(float64)
						lng, _ = check.location["longitude"].(float64)
						registerDriverLocation(check.refID, lat, lng)
					}
					addCandidateToPool(check.refID, lat, lng)
					updateDriverStatus(driverPhone, true)
				}
				drvState, ok := loadUserState(driverPhone)
				if ok && drvState != nil {
					drvState.IsOnline = true
					saveUserState(driverPhone, drvState)
				}
			}()
		}

	case events.EventTripAborted:
		var data events.TripAbortedData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		driverPhone := resolvePhone(data.DriverRefID)
		customerPhone := resolvePhone(data.CustomerRefID)
		if strings.HasPrefix(driverPhone, "62") {
			sendMessage(client, waJID(customerPhone), "Trip dibatalkan (abort).")
			sendMessage(client, waJID(driverPhone), fmt.Sprintf("Trip aborted. Reason: %s", data.Reason))
			deletePartner(driverPhone)
			deletePartner(customerPhone)
		}

	case events.EventTripCancelled:
		var data events.TripCancelledData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		driverPhone := resolvePhone(data.DriverRefID)
		customerPhone := resolvePhone(data.CustomerRefID)
		if strings.HasPrefix(driverPhone, "62") {
			closeChatSession(data.TripID)
			deletePartner(driverPhone)
			deletePartner(customerPhone)
			if drvState, ok := loadUserState(driverPhone); ok && drvState != nil {
				drvState.CurrentBidPrice = 0
				drvState.LastBidder = ""
				saveUserState(driverPhone, drvState)
			}
			if custState, ok := loadUserState(customerPhone); ok && custState != nil {
				custState.CurrentBidPrice = 0
				custState.LastBidder = ""
				saveUserState(customerPhone, custState)
			}
		}

	case events.EventMatchTimeout:
		var data events.MatchTimeoutData
		if json.Unmarshal(dataBytes, &data) != nil {
			return
		}
		custPhone := resolvePhone(data.CustomerRefID)
		fmt.Printf("[MATCH-TIMEOUT] customer=%s order=%s\n", custPhone, data.OrderID)
		cancelMatchRequest(data.OrderID)
		if strings.HasPrefix(custPhone, "62") {
			if custState, ok := loadUserState(custPhone); ok && custState != nil {
				custState.ActiveTrip = ""
				custState.CurrentBidPrice = 0
				custState.LastBidder = ""
				saveUserState(custPhone, custState)
			}
		sendMessage(client, waJID(custPhone), "No drivers available right now.\nType *?retry* to retry with the same route.")
		}
	}
}

func menuPrompt(context string) string {
	switch context {
	case "gender":
		return "q: Male\nw: Female"
	case "confirm_ride":
		return "q: Go\nw: Cancel"
	case "match":
		return "q: Accept\nw: Reject (with reason)\n\n⏱ 1 min to respond"
	case "trip":
		return ""
	case "complete":
		return "q: Paid\nw: Debt"
	case "rate":
		return "q: Rate (q 1-5)\nw: Skip"
	case "debt":
		return "q: Pay\nw: Confirm paid\ne: Rate driver (e 1-5)"
	case "service_type":
		return "q: Anjem (escort ride)\nw: Jastip (delivery)"
	}
	return ""
}

func handleMenuLetter(client *whatsmeow.Client, sender string, senderJID types.JID, state *UserState, letter string, rest string) bool {
	mc := state.MenuContext
	switch mc {
	case "gender":
		switch letter {
		case "q":
			personalizeUser(sender, state.Name, state.Nickname, "Male")
			state.Step = StepNormal
			state.MenuContext = ""
			if state.IsDriver {
				sendMessage(client, senderJID, "Profile complete! You can now go online.")
			} else {
				sendMessage(client, senderJID, "Profile complete! Share your *pickup location* (WA share location) to order a ride.")
			}
			return true
		case "w":
			personalizeUser(sender, state.Name, state.Nickname, "Female")
			state.Step = StepNormal
			state.MenuContext = ""
			if state.IsDriver {
				sendMessage(client, senderJID, "Profile complete! You can now go online.")
			} else {
				sendMessage(client, senderJID, "Profile complete! Share your *pickup location* (WA share location) to order a ride.")
			}
			return true
		}

	case "confirm_ride":
		switch letter {
		case "q":
			state.MenuContext = ""
			if state.HasDebt {
				sendMessage(client, senderJID, "You have *outstanding debt*. Please pay first.\n\nq: Pay\nw: Confirm paid\ne: Rate driver (e 1-5)")
				state.MenuContext = "debt"
				return true
			}
			if !state.PickupSet || !state.DestSet {
				sendMessage(client, senderJID, "Missing pickup or destination. Share your *pickup location* to restart.")
				state.Step = StepAskPickup
				return true
			}
			if state.ServiceType == "" {
				state.ServiceType = "anjem"
			}
			state.LastPickupLat = state.PickupLat
			state.LastPickupLng = state.PickupLng
			state.LastDestLat = state.DestLat
			state.LastDestLng = state.DestLng
			state.LastServiceType = state.ServiceType
			sendMessage(client, senderJID, "Searching for drivers...")
			result := requestRideWithDetails(sender, state.PickupLat, state.PickupLng, state.DestLat, state.DestLng, state.ServiceType, state.CustomRequests, state.ItemDetail, state.GenderPref)
			if result != nil {
				orderID, _ := result["order_id"].(string)
				var link string
				if orderID != "" {
					link = buildTripLink(sender, orderID, state.PickupLat, state.PickupLng, state.DestLat, state.DestLng, state.ServiceType)
				}
				if link != "" {
					sendMessage(client, senderJID, "Pesanan kamu sedang dicari driver! Pantau, tawar harga, chat, dan lacak driver di sini:\n"+link)
				} else {
					sendMessage(client, senderJID, "Search started! Waiting for a driver match...")
				}
			} else {
				sendMessage(client, senderJID, "Failed to request ride. Type *?retry* to try again.")
			}
			state.Step = StepNormal
			state.PickupSet = false
			state.DestSet = false
			state.CustomRequests = ""
			state.ItemDetail = ""
			state.GenderPref = ""
			return true
		case "w":
			state.Step = StepNormal
			state.PickupSet = false
			state.DestSet = false
			state.MenuContext = ""
			sendMessage(client, senderJID, "Ride cancelled. Share a new pickup location to start over.")
			return true
		}

	case "match":
		if !state.IsDriver {
			return false
		}
		switch letter {
		case "q":
			trip := getDriverActiveTrip(sender)
			if trip != nil && trip["status"].(string) == "pending_acceptance" {
				if acceptTrip(trip["id"].(string)) {
					state.ActiveTrip = trip["id"].(string)
					state.LastBidder = ""
					drvPhone := sender
					custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
					if custPhone != "" && strings.HasPrefix(custPhone, "62") {
						savePartner(drvPhone, custPhone)
						savePartner(custPhone, drvPhone)
					}
					sessionID := saveChatSession(trip["id"].(string), trip["order_id"].(string), drvPhone, custPhone)
					state.ChatSessionID = sessionID
					if custState, ok := loadUserState(custPhone); ok && custState != nil {
						custState.ChatSessionID = sessionID
						custState.CurrentBidPrice = state.CurrentBidPrice
						custState.LastBidder = ""
						saveUserState(custPhone, custState)
					}
					sendMessage(client, senderJID, fmt.Sprintf("Trip accepted! Rp %.0f\n\n?bid <amount> <reason> — Bid a price\n?start — Start trip", state.CurrentBidPrice))
					if custPhone != "" {
						notifyWABA(custPhone, "driver_accepted", state.CurrentBidPrice, state.ActiveTrip)
					}
					saveChatMessage(drvPhone, "driver", "system", "Trip accepted")
					saveChatMessage(custPhone, "customer", "system", "Trip accepted")
					state.MenuContext = "trip"
				} else {
					sendMessage(client, senderJID, "Failed to accept trip.")
				}
			} else if state.ActiveTrip != "" {
				for i := 0; i < 5; i++ {
					time.Sleep(300 * time.Millisecond)
					trip = getDriverActiveTrip(sender)
					if trip != nil && trip["status"].(string) == "pending_acceptance" {
						if acceptTrip(trip["id"].(string)) {
							state.ActiveTrip = trip["id"].(string)
							state.LastBidder = ""
							drvPhone := sender
							custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
							if custPhone != "" && strings.HasPrefix(custPhone, "62") {
								savePartner(drvPhone, custPhone)
								savePartner(custPhone, drvPhone)
							}
							sessionID := saveChatSession(trip["id"].(string), trip["order_id"].(string), drvPhone, custPhone)
							state.ChatSessionID = sessionID
							if custState, ok := loadUserState(custPhone); ok && custState != nil {
								custState.ChatSessionID = sessionID
								custState.CurrentBidPrice = state.CurrentBidPrice
								custState.LastBidder = ""
								saveUserState(custPhone, custState)
							}
							sendMessage(client, senderJID, fmt.Sprintf("Trip accepted! Rp %.0f\n\n?bid <amount> <reason> — Bid a price\n?start — Start trip", state.CurrentBidPrice))
							if custPhone != "" {
								notifyWABA(custPhone, "driver_accepted", state.CurrentBidPrice, state.ActiveTrip)
							}
							saveChatMessage(drvPhone, "driver", "system", "Trip accepted")
							saveChatMessage(custPhone, "customer", "system", "Trip accepted")
							state.MenuContext = "trip"
							return true
						}
					}
				}
				check := checkDriverGpsAndLocation(sender)
				if check.refID != "" {
					markDriverOffline(check.refID)
					time.Sleep(500 * time.Millisecond)
					markDriverOnline(check.refID)
					if check.gpsActive && check.location != nil {
						lat, _ := check.location["latitude"].(float64)
						lng, _ := check.location["longitude"].(float64)
						addCandidateToPool(check.refID, lat, lng)
					}
				}
				state.ActiveTrip = ""
				state.CurrentBidPrice = 0
				state.LastBidder = ""
				state.MenuContext = ""
				sendMessage(client, senderJID, "Trip accepted. Waiting for next trip...")
			}
			return true
		case "w":
			reason := rest
			if reason == "" {
				state.PendingAction = "reject_awaiting_reason"
				sendMessage(client, senderJID, "Reason for rejection?")
				return true
			}
			doRejectMatch(client, sender, senderJID, state, reason)
			return true
		}

	case "complete":
		if !state.IsDriver {
			return false
		}
		payStatus := ""
		switch letter {
		case "q":
			payStatus = "paid"
		case "w":
			if rest != "" {
				var paidAmt float64
				fmt.Sscanf(rest, "%f", &paidAmt)
				if paidAmt > 0 {
					trip := getDriverActiveTrip(sender)
					if trip != nil && trip["status"].(string) == "in_progress" {
						tripValue, _ := trip["final_price"].(float64)
						if tripValue == 0 {
							tripValue, _ = trip["current_bid_price"].(float64)
						}
						if paidAmt < 0 || paidAmt > tripValue {
							sendMessage(client, senderJID, fmt.Sprintf("Paid amount must be between 0 and Rp %.0f (trip value).", tripValue))
							return true
						}
						debtAmount := tripValue - paidAmt
						if completeTripWithDebt(trip["id"].(string), debtAmount) {
							custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
							if custPhone != "" {
								custState, _ := loadUserState(custPhone)
								if custState == nil {
									custState = &UserState{}
								}
								if debtAmount > 0 {
									custState.HasDebt = true
									custState.DebtAmount = debtAmount
								}
								saveUserState(custPhone, custState)
							}
							state.ActiveTrip = ""
							state.ChatSessionID = ""
							state.CurrentBidPrice = 0
							state.LastBidder = ""
							state.MenuContext = "rate"
							state.PendingAction = "auto_ready_after_rate"
						} else {
							sendMessage(client, senderJID, "Failed to complete trip.")
						}
						return true
					}
				}
			}
			state.PendingAction = "complete_debt_awaiting_amount"
			trip := getDriverActiveTrip(sender)
			tripValue := 0.0
			if trip != nil {
				tripValue, _ = trip["final_price"].(float64)
				if tripValue == 0 {
					tripValue, _ = trip["current_bid_price"].(float64)
				}
			}
			sendMessage(client, senderJID, fmt.Sprintf("Paid amount? (trip value: Rp %.0f)", tripValue))
			return true
		}
		if payStatus == "" {
			sendMessage(client, senderJID, "Unrecognized.\nq: Paid\nw: Debt")
			return true
		}
		trip := getDriverActiveTrip(sender)
		if trip != nil && trip["status"].(string) == "in_progress" {
			if completeTrip(trip["id"].(string), payStatus) {
				custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
				if payStatus == "debt" && custPhone != "" {
					custState, _ := loadUserState(custPhone)
					if custState == nil {
						custState = &UserState{}
					}
					custState.HasDebt = true
					saveUserState(custPhone, custState)
				}
				state.ActiveTrip = ""
				state.ChatSessionID = ""
				state.CurrentBidPrice = 0
				state.LastBidder = ""
				state.MenuContext = "rate"
				state.PendingAction = "auto_ready_after_rate"
			} else {
				sendMessage(client, senderJID, "Failed to complete trip.")
			}
		} else {
			sendMessage(client, senderJID, "No in-progress trip to complete.")
		}
		return true

	case "rate":
		switch letter {
		case "q":
			if rest == "" {
				state.PendingAction = "rate_awaiting_score"
				sendMessage(client, senderJID, "Score? (1-5)")
				return true
			}
			var score int
			fmt.Sscanf(rest, "%d", &score)
			if score < 1 || score > 5 {
				sendMessage(client, senderJID, "Enter a number 1-5")
				return true
			}
			doRate(client, sender, senderJID, state, score)
			state.MenuContext = ""
			if state.IsDriver && state.IsOnline {
				sendMessage(client, senderJID, "You are *READY* waiting for orders.\n`?unready` to go offline.")
			} else if state.IsDriver {
				sendMessage(client, senderJID, "Rating submitted! You can continue grabbing orders.\nType `?ready` to go online.")
			} else if !state.IsDriver {
				sendMessage(client, senderJID, "Share your *pickup location* (WA share location) to order a ride, or type `!help` for more options.")
			}
			return true
		case "w":
			sendMessage(client, senderJID, "Rating skipped. You can rate later with ?rating 1-5 <name>.")
			state.MenuContext = ""
			if state.IsDriver && state.IsOnline {
				sendMessage(client, senderJID, "You are *READY* waiting for orders.\n`?unready` to go offline.")
			} else if state.IsDriver {
				sendMessage(client, senderJID, "You can continue grabbing orders.\nType `?ready` to go online.")
			} else if !state.IsDriver {
				sendMessage(client, senderJID, "Share your *pickup location* (WA share location) to order a ride, or type `!help` for more options.")
			}
			return true
		}

	case "debt":
		switch letter {
		case "q":
			sendMessage(client, senderJID, fmt.Sprintf("Please pay your debt of Rp %.0f to the driver.\nAfter paying, type *w* to confirm payment.\n\n_Payment QR/instructions will be available soon._", state.DebtAmount))
			return true
		case "w":
			state.HasDebt = false
			state.DebtAmount = 0
			state.MenuContext = ""
			sendMessage(client, senderJID, "Debt cleared! You can now order rides again.\nShare a *pickup location* to start.")
			return true
		case "e":
			if rest == "" {
				state.PendingAction = "rate_awaiting_score_debt"
				sendMessage(client, senderJID, "Score? (1-5)")
				return true
			}
			var score int
			fmt.Sscanf(rest, "%d", &score)
			if score < 1 || score > 5 {
				sendMessage(client, senderJID, "Enter a number 1-5")
				return true
			}
			doRate(client, sender, senderJID, state, score)
			sendMessage(client, senderJID, fmt.Sprintf("Rating submitted! ⭐%d\nYou still have outstanding debt of Rp %.0f.\n\nq: Pay\nw: Confirm paid", score, state.DebtAmount))
			return true
		}

	case "service_type":
		switch letter {
		case "q":
			state.ServiceType = "anjem"
			state.MenuContext = ""
			state.Step = StepAskCustomReq
			sendMessage(client, senderJID, "*Anjem* selected.\nAda request khusus? (gender pref, helm, dll)\n\nq: Skip\nOr type your request:")
			return true
		case "w":
			state.ServiceType = "jastip"
			state.MenuContext = ""
			state.Step = StepAskItemDetail
			sendMessage(client, senderJID, "*Jastip* selected.\nBarang apa yang mau diantar?\n\nq: Skip\nOr type the item description:")
			return true
		}
	}

	sendMessage(client, senderJID, "Unrecognized.\n"+menuPrompt(mc))
	return true
}

func handlePendingAction(client *whatsmeow.Client, sender string, senderJID types.JID, state *UserState, msgText string) bool {
	pa := state.PendingAction
	state.PendingAction = ""

	switch pa {
	case "bid_awaiting_params":
		parts := strings.SplitN(msgText, " ", 2)
		if len(parts) < 2 {
			state.PendingAction = "bid_awaiting_params"
			sendMessage(client, senderJID, "Need amount & reason. (e.g., 18000 jauh pak)")
			return true
		}
		doBid(client, sender, senderJID, state, msgText)
		return true

	case "reject_awaiting_reason":
		doRejectMatch(client, sender, senderJID, state, msgText)
		return true

	case "rate_awaiting_score":
		var score int
		fmt.Sscanf(msgText, "%d", &score)
		if score < 1 || score > 5 {
			state.PendingAction = "rate_awaiting_score"
			sendMessage(client, senderJID, "Enter a number 1-5")
			return true
		}
		doRate(client, sender, senderJID, state, score)
		state.MenuContext = ""
		return true

	case "rate_awaiting_score_debt":
		var score int
		fmt.Sscanf(msgText, "%d", &score)
		if score < 1 || score > 5 {
			state.PendingAction = "rate_awaiting_score_debt"
			sendMessage(client, senderJID, "Enter a number 1-5")
			return true
		}
		doRate(client, sender, senderJID, state, score)
		state.MenuContext = "debt"
		sendMessage(client, senderJID, fmt.Sprintf("Rating submitted! ⭐%d\nYou still have outstanding debt of Rp %.0f.\n\nq: Pay\nw: Confirm paid", score, state.DebtAmount))
		return true

	case "complete_debt_awaiting_amount":
		var paidAmount float64
		fmt.Sscanf(msgText, "%f", &paidAmount)
		trip := getDriverActiveTrip(sender)
		if trip == nil || trip["status"].(string) != "in_progress" {
			sendMessage(client, senderJID, "No in-progress trip to complete.")
			return true
		}
		tripValue, _ := trip["final_price"].(float64)
		if tripValue == 0 {
			tripValue, _ = trip["current_bid_price"].(float64)
		}
		if paidAmount < 0 || paidAmount > tripValue {
			sendMessage(client, senderJID, fmt.Sprintf("Paid amount must be between 0 and Rp %.0f (trip value).", tripValue))
			return true
		}
		debtAmount := tripValue - paidAmount
		if completeTripWithDebt(trip["id"].(string), debtAmount) {
			custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
			if custPhone != "" {
				custState, _ := loadUserState(custPhone)
				if custState == nil {
					custState = &UserState{}
				}
				if debtAmount > 0 {
					custState.HasDebt = true
				}
				saveUserState(custPhone, custState)
			}
			state.ActiveTrip = ""
			state.ChatSessionID = ""
			state.CurrentBidPrice = 0
			state.LastBidder = ""
			state.MenuContext = "rate"
			state.PendingAction = "auto_ready_after_rate"
		} else {
			sendMessage(client, senderJID, "Failed to complete trip.")
		}
		return true
	}

	return false
}

func doBid(client *whatsmeow.Client, sender string, senderJID types.JID, state *UserState, params string) {
	parts := strings.SplitN(params, " ", 2)
	if len(parts) < 2 {
		sendMessage(client, senderJID, "Need amount & reason. (e.g., ?bid 18000 jauh pak)")
		return
	}
	var amount float64
	fmt.Sscanf(parts[0], "%f", &amount)
	reason := parts[1]
	if amount < 5000 {
		sendMessage(client, senderJID, "Minimum bid is Rp 5,000")
		return
	}
	if state.IsDriver {
		trip := getDriverActiveTrip(sender)
		if trip == nil {
			sendMessage(client, senderJID, "No active trip to bid on.")
			return
		}
		status := trip["status"].(string)
		if status != "bargaining" && status != "accepted" {
			sendMessage(client, senderJID, fmt.Sprintf("Cannot bid — trip status is %s.", status))
			return
		}
		if status == "accepted" {
			state.LastBidder = ""
		}
		if state.LastBidder == "driver" {
			sendMessage(client, senderJID, "You already made a bid. Wait for the customer to respond.")
			return
		}
		if bidTrip(trip["id"].(string), sender, "driver", amount, reason) {
			state.CurrentBidPrice = amount
			state.LastBidder = "driver"
			custPhone := resolvePhone(fmt.Sprintf("%v", trip["customer_ref_id"]))
			if custPhone != "" {
					sendMessage(client, waJID(custPhone), fmt.Sprintf("Driver bids Rp %.0f\nReason: %s\n\n?deal — Accept current price\n?bid <amount> <reason> — Counter bid", amount, reason))
					if custState, ok := loadUserState(custPhone); ok && custState != nil {
						custState.CurrentBidPrice = amount
						custState.LastBidder = "driver"
						saveUserState(custPhone, custState)
					}
			}
			sendMessage(client, senderJID, fmt.Sprintf("Bid sent: Rp %.0f\nReason: %s\nWaiting for customer response...", amount, reason))
		} else {
			sendMessage(client, senderJID, "Failed to place bid.")
		}
	} else {
		if state.ActiveTrip == "" {
			sendMessage(client, senderJID, "No active trip to bid on.")
			return
		}
		tripData := getCustomerActiveTrip(sender)
		if tripData == nil {
			sendMessage(client, senderJID, "No active trip found.")
			return
		}
		status := tripData["status"].(string)
		if status != "bargaining" && status != "accepted" {
			sendMessage(client, senderJID, fmt.Sprintf("Cannot bid — trip status is %s.", status))
			return
		}
		if status == "accepted" {
			state.LastBidder = ""
		}
		if state.LastBidder == "customer" {
			sendMessage(client, senderJID, "You already made a bid. Wait for the driver to respond.")
			return
		}
		if bidTrip(tripData["id"].(string), sender, "customer", amount, reason) {
			state.CurrentBidPrice = amount
			state.LastBidder = "customer"
			drvPhone := resolvePhone(fmt.Sprintf("%v", tripData["driver_ref_id"]))
			if drvPhone != "" {
						sendMessage(client, waJID(drvPhone), fmt.Sprintf("Customer bids Rp %.0f\nReason: %s\n\n?deal — Accept current price\n?bid <amount> <reason> — Counter bid", amount, reason))
						if drvState, ok := loadUserState(drvPhone); ok && drvState != nil {
							drvState.CurrentBidPrice = amount
							drvState.LastBidder = "customer"
							saveUserState(drvPhone, drvState)
						}
			}
			sendMessage(client, senderJID, fmt.Sprintf("Bid sent: Rp %.0f\nReason: %s\nWaiting for driver response...", amount, reason))
		} else {
			sendMessage(client, senderJID, "Failed to place bid.")
		}
	}
}

func doRejectMatch(client *whatsmeow.Client, sender string, senderJID types.JID, state *UserState, reason string) {
	trip := getDriverActiveTrip(sender)
	driverRefID := checkDriverGpsAndLocation(sender).refID
	orderID := state.ActiveTrip
	custPhone := state.MatchedCustomerPhone

	if trip != nil && (trip["status"].(string) == "pending_acceptance" || trip["status"].(string) == "bargaining") {
		rejectTrip(trip["id"].(string), reason)
	}

	if orderID != "" && driverRefID != "" {
		rejectMatchRequest(orderID, driverRefID)
	}

	if driverRefID != "" {
		markDriverOffline(driverRefID)
		time.Sleep(500 * time.Millisecond)
		markDriverOnline(driverRefID)
		check := checkDriverGpsAndLocation(sender)
		if check.gpsActive && check.location != nil {
			lat, _ := check.location["latitude"].(float64)
			lng, _ := check.location["longitude"].(float64)
			addCandidateToPool(check.refID, lat, lng)
		}
	}

	state.ActiveTrip = ""
	state.CurrentBidPrice = 0
	state.LastBidder = ""
	state.MenuContext = ""
	state.MatchedCustomerPhone = ""

	if reason != "" {
		sendMessage(client, senderJID, fmt.Sprintf("Rejected: %s\nWaiting for next trip...", reason))
	} else {
		sendMessage(client, senderJID, "Rejected. Waiting for next trip...")
	}
	if custPhone != "" {
		sendMessage(client, waJID(custPhone), "Driver rejected. Searching for another driver...\nPlease wait.")
	}
	fmt.Printf("[MATCH-REJECT] driver=%s order=%s reason=%s\n", sender, orderID, reason)
}

func doRate(client *whatsmeow.Client, sender string, senderJID types.JID, state *UserState, score int) {
	var tripID, orderID, rateeID string
	var raterType string
	raterID := resolveUserID(sender)
	if raterID == "" {
		raterID = sender
	}
	if state.IsDriver {
		raterType = "driver"
		trip := getDriverActiveTrip(sender)
		if trip == nil {
			trips, _ := getDriverRecentTrips(sender, 1)
			if len(trips) == 0 {
				sendMessage(client, senderJID, "No recent trip to rate.")
				return
			}
			trip = trips[0]
		}
		tripID, _ = trip["id"].(string)
		orderID, _ = trip["order_id"].(string)
		rateeID, _ = trip["customer_ref_id"].(string)
		log.Printf("[RATING] doRate driver tripID=%s orderID=%s rateeID=%s raterID=%s", tripID, orderID, rateeID, raterID)
	} else {
		raterType = "customer"
		trip := getCustomerActiveTrip(sender)
		if trip == nil {
			trips, _ := getCustomerRecentTrips(sender, 1)
			if len(trips) == 0 {
				sendMessage(client, senderJID, "No recent trip to rate.")
				return
			}
			trip = trips[0]
		}
		tripID, _ = trip["id"].(string)
		orderID, _ = trip["order_id"].(string)
		rateeID, _ = trip["driver_ref_id"].(string)
	}
	if rateeID == "" {
		sendMessage(client, senderJID, "Cannot find trip to rate.")
		return
	}
	result := submitRating(raterID, raterType, rateeID, tripID, orderID, score, "")
	if result {
		if score <= 2 {
			sendMessage(client, senderJID, fmt.Sprintf("Rating %d submitted. Will be reviewed by admin.", score))
		} else {
			sendMessage(client, senderJID, fmt.Sprintf("Rating %d submitted. Thank you!", score))
		}
	} else {
		sendMessage(client, senderJID, "Failed to submit rating.")
	}
}

func reconcileActiveTrips() {
	url := fmt.Sprintf("%s/trips?status=in_progress", apiGatewayURL)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("[RECONCILE] failed to query active trips: %v\n", err)
		return
	}
	defer resp.Body.Close()
	var trips []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&trips)

	for _, trip := range trips {
		driverRef, _ := trip["driver_ref_id"].(string)
		customerRef, _ := trip["customer_ref_id"].(string)
		driverPhone := resolvePhone(driverRef)
		customerPhone := customerRef
		if strings.HasPrefix(customerRef, "62") {
			customerPhone = customerRef
		}
		if !strings.HasPrefix(driverPhone, "62") || !strings.HasPrefix(customerPhone, "62") {
			continue
		}
		savePartner(driverPhone, customerPhone)
		savePartner(customerPhone, driverPhone)
		if _, ok := loadUserState(driverPhone); !ok {
			saveUserState(driverPhone, &UserState{Step: StepNormal, IsDriver: true})
		}
		if _, ok := loadUserState(customerPhone); !ok {
			saveUserState(customerPhone, &UserState{Step: StepNormal})
		}
		fmt.Printf("[RECONCILE] restored pair: driver=%s customer=%s\n", driverPhone, customerPhone)
	}
	fmt.Printf("[RECONCILE] restored %d active trip pairs\n", len(trips))
}

func reconcileDriverStates() {
	fmt.Println("[RECONCILE-DRV] syncing bot state with user-service driver_status...")

	drvResp, err := http.Get(fmt.Sprintf("%s/drivers/all", apiGatewayURL))
	if err != nil {
		fmt.Printf("[RECONCILE-DRV] failed to fetch drivers: %v\n", err)
		return
	}
	defer drvResp.Body.Close()
	var drivers []map[string]interface{}
	json.NewDecoder(drvResp.Body).Decode(&drivers)

	poolResp, err := http.Get(fmt.Sprintf("%s/match/pool", apiGatewayURL))
	if err != nil {
		fmt.Printf("[RECONCILE-DRV] failed to fetch pool: %v\n", err)
		return
	}
	defer poolResp.Body.Close()
	var pool []map[string]interface{}
	json.NewDecoder(poolResp.Body).Decode(&pool)
	poolSet := map[string]bool{}
	for _, p := range pool {
		if ref, ok := p["ref_id"].(string); ok {
			poolSet[ref] = true
		}
	}

	synced := 0
	for _, d := range drivers {
		id, _ := d["id"].(string)
		driverStatus, _ := d["driver_status"].(string)
		var phone string
		if phones, ok := d["phone"].([]interface{}); ok && len(phones) > 0 {
			phone, _ = phones[0].(string)
		}
		if phone == "" {
			phone, _ = d["phone"].(string)
		}
		if phone == "" || id == "" {
			continue
		}

		state, _ := loadUserState(phone)
		if state == nil {
			state = &UserState{Step: StepNormal, IsDriver: true, UserID: id}
		}
		state.IsDriver = true
		state.UserID = id

		isReadyInDB := driverStatus == "READY" || driverStatus == "ON_JOB"
		if state.IsOnline != isReadyInDB {
			fmt.Printf("[RECONCILE-DRV] %s: IsOnline=%v → %v (driver_status=%s)\n", phone, state.IsOnline, isReadyInDB, driverStatus)
			state.IsOnline = isReadyInDB
			synced++
		}
		saveUserState(phone, state)

		if isReadyInDB && !poolSet[id] {
			check := checkDriverGpsAndLocation(phone)
			if check.refID != "" && check.gpsActive && check.location != nil {
				lat, _ := check.location["latitude"].(float64)
				lng, _ := check.location["longitude"].(float64)
				markDriverOnline(check.refID)
				addCandidateToPool(check.refID, lat, lng)
				fmt.Printf("[RECONCILE-DRV] re-added %s to pool (%.4f,%.4f)\n", phone, lat, lng)
			} else {
				fmt.Printf("[RECONCILE-DRV] %s READY but no GPS/location, skipping pool add\n", phone)
			}
		} else if isReadyInDB && poolSet[id] {
			check := checkDriverGpsAndLocation(phone)
			if check.refID != "" && check.gpsActive && check.location != nil {
				lat, _ := check.location["latitude"].(float64)
				lng, _ := check.location["longitude"].(float64)
				addCandidateToPool(check.refID, lat, lng)
			}
		}
	}
	fmt.Printf("[RECONCILE-DRV] synced %d states, %d drivers checked\n", synced, len(drivers))
}

func main() {
	config.LoadConfig()
	apiGatewayURL = config.GetEnv("API_GATEWAY_URL", "http://localhost:9080")
	wabaURL = config.GetEnv("WABA_SERVICE_URL", "http://localhost:9089")
	webAppBaseURL = config.GetEnv("WEB_APP_BASE_URL", "")
	redisAddr := config.GetEnv("REDIS_ADDR", "localhost:6379")
	botPort := config.GetEnv("BOT_PORT", "9088")

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "whatsapp-bot"})
		})
		mux.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			connected := waClient != nil && waClient.IsConnected()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"service":          "whatsapp-bot",
				"connected":        connected,
				"user_states":      len(userStates),
				"active_trips":     len(activeTripPartner),
				"gps_relay_active": gpsRelayActive.Load() == 1,
			})
		})
		mux.HandleFunc("/admin/send-text", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				Phone string `json:"phone"`
				Text  string `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Phone == "" || req.Text == "" {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if waClient == nil || !waClient.IsConnected() {
				http.Error(w, "bot not connected", http.StatusServiceUnavailable)
				return
			}
			jid := waJID(req.Phone)
			sendMessage(waClient, jid, req.Text)
			logWAChat("out", req.Phone, "BOT", req.Text)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		})
		mux.HandleFunc("/admin/reset-state", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			phone := r.URL.Query().Get("phone")
			if phone != "" {
				deleteUserState(phone)
				deletePartner(phone)
				deleteLastMsg(phone)
				deletePartnerReverse(phone)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "phone": phone})
				return
			}
			count := len(userStates) + len(activeTripPartner) + len(lastCustomerMessage)
			resetAllState()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "ok",
				"cleared": count,
			})
		})
		srv := &http.Server{Addr: ":" + botPort, Handler: mux}
		log.Println("WhatsApp Bot HTTP on :" + botPort)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Bot HTTP server error: %v", err)
		}
	}()

	initChatMongo()

	initRedisClient(config.GetEnv("REDIS_ADDR", "localhost:6379"))
	reconcileActiveTrips()
	reconcileDriverStates()

	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Bot panic recovered: %v, restarting in 5s...\n", r)
					time.Sleep(5 * time.Second)
				}
			}()
			err := runBot(redisAddr)
			if err != nil {
				fmt.Printf("Bot crashed: %v, restarting in 5s...\n", err)
				time.Sleep(5 * time.Second)
			} else {
				fmt.Println("Bot stopped cleanly, restarting in 3s...")
				time.Sleep(3 * time.Second)
			}
		}()
	}
}

func runBot(redisAddr string) error {
	dbLog := waLog.Stdout("Database", "WARN", true)
	container, err := sqlstore.New(context.Background(), "sqlite3", "file:examplestore.db?_foreign_keys=on", dbLog)
	if err != nil {
		panic(err)
	}

	deviceRes, err := container.GetFirstDevice(context.Background())
	if err != nil {
		panic(err)
	}

	clientLog := waLog.Stdout("Client", "WARN", true)
	client := whatsmeow.NewClient(deviceRes, clientLog)
	client.EnableAutoReconnect = true
	client.AutoReconnectHook = func(err error) bool {
		fmt.Printf("Auto-reconnect attempt: %v\n", err)
		return true
	}
	waClient = client

	client.AddEventHandler(func(evt interface{}) {
		eventHandler(evt, client)
	})

	initRedisClient(redisAddr)
	go startRedisEventRelay(context.Background(), rdb, client)
	go startGpsStatusRelay(context.Background(), rdb, client)
	go startDriverStatusRelay(context.Background(), rdb, client)
	go func() {
		for {
			time.Sleep(60 * time.Second)
			reconcileDriverStates()
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Runtime toggle: the whatsmeow client only connects/processes while the
	// Redis flag studex:config:whatsmeow_enabled is "true" (default true).
	// A poll loop reconciles desired vs. actual connection state so the flag
	// can be flipped from the dashboard without restarting the process. A
	// pubsub subscription on studex:config:whatsmeow wakes the loop instantly.
	wake := make(chan struct{}, 1)
	go watchWhatsmeowFlag(wake)

	if !isWhatsmeowEnabled() {
		fmt.Println("Whatsmeow disabled by config flag; idling (HTTP/admin API still served)")
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		reconcileWhatsmeowConnection(client)
		select {
		case <-c:
			if client.IsConnected() {
				client.Disconnect()
			}
			return nil
		case <-wake:
			// flag changed via pubsub, reconcile immediately
		case <-ticker.C:
			// periodic reconcile / connection health check
		}
	}
}

// isWhatsmeowEnabled reads the toggle flag from Redis. Default is enabled
// (true) when the key is unset or Redis is unavailable.
func isWhatsmeowEnabled() bool {
	if rdb == nil {
		return true
	}
	val, err := rdb.Get(context.Background(), "studex:config:whatsmeow_enabled").Result()
	if err != nil {
		return true // key unset or redis error -> default enabled
	}
	return val != "false"
}

// watchWhatsmeowFlag subscribes to the config pubsub channel and signals the
// reconcile loop whenever the flag changes.
func watchWhatsmeowFlag(wake chan<- struct{}) {
	if rdb == nil {
		return
	}
	sub := rdb.Subscribe(context.Background(), "studex:config:whatsmeow")
	ch := sub.Channel()
	for range ch {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// reconcileWhatsmeowConnection brings the whatsmeow client's actual connection
// state in line with the desired state from the toggle flag. It is safe against
// double-connect / double-disconnect.
func reconcileWhatsmeowConnection(client *whatsmeow.Client) {
	enabled := isWhatsmeowEnabled()
	connected := client.IsConnected()

	if enabled && !connected {
		fmt.Println("Whatsmeow enabled: connecting client...")
		connectWhatsmeow(client)
	} else if !enabled && connected {
		fmt.Println("Whatsmeow disabled: disconnecting client...")
		client.Disconnect()
	}
}

// connectWhatsmeow connects the whatsmeow client, handling first-time QR
// pairing when no device is stored.
func connectWhatsmeow(client *whatsmeow.Client) {
	if client.Store.ID == nil {
		qrChan, _ := client.GetQRChannel(context.Background())
		if err := client.Connect(); err != nil {
			fmt.Printf("Whatsmeow connect (QR) failed: %v\n", err)
			return
		}
		go func() {
			for evt := range qrChan {
				if evt.Event == "code" {
					qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
					fmt.Println("QR code generated. Please scan with your WhatsApp app.")
					if rdb != nil {
						rdb.Set(context.Background(), "studex:wa:qr_code", evt.Code, 5*time.Minute)
					}
				} else {
					fmt.Println("QR channel event:", evt.Event)
					if evt.Event == "connected" && rdb != nil {
						rdb.Del(context.Background(), "studex:wa:qr_code")
					}
				}
			}
		}()
		return
	}

	if err := client.Connect(); err != nil {
		fmt.Printf("Whatsmeow connect failed: %v\n", err)
		return
	}

	// Wait until the client is actually connected (best-effort).
	for i := 0; i < 30; i++ {
		if client.IsConnected() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if client.IsConnected() {
		fmt.Println("WhatsApp bot connected!")
	} else {
		fmt.Println("Warning: client not connected after 30s")
	}
}
