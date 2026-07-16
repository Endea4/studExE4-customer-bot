package events

import (
	"encoding/json"
	"time"
)

type Event struct {
	Type   string      `json:"type"`
	Source string      `json:"source"`
	Time   time.Time   `json:"time"`
	Data   interface{} `json:"data"`
}

// ParseEvent unmarshals a raw Redis payload into Event, then extracts Data into the target.
func ParseEvent(payload []byte, target interface{}) error {
	var raw struct {
		Type   string          `json:"type"`
		Source string          `json:"source"`
		Time   time.Time       `json:"time"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return err
	}
	return json.Unmarshal(raw.Data, target)
}

const (
	EventMatchCompleted    = "match.completed"
	EventMatchTimeout      = "match.timeout"
	EventMatchCancelled    = "match.cancelled"
	EventTripCreated       = "trip.created"
	EventTripStarted       = "trip.started"
	EventTripCompleted     = "trip.completed"
	EventTripCancelled     = "trip.cancelled"
	EventTripAborted       = "trip.aborted"
	EventTripBargaining    = "trip.bargaining"
	EventTripDeal          = "trip.deal"
	EventLocationUpdated   = "location.updated"
	EventRatingSubmitted   = "rating.submitted"
	EventRatingConfirmed   = "rating.confirmed"
	EventRatingRejected    = "rating.rejected"
	EventDriverStatusChanged = "driver.status_changed"
	EventRematchRequested    = "rematch_requested"
)

const (
	TopicMatchEvents = "studex.match.events"
	TopicTripEvents  = "studex.trip.events"
	TopicRatingEvents = "studex.rating.events"

	RedisChannelLocationUpdates = "studex:location:updates"
	RedisChannelMatchEvents     = "studex:match:events"
	RedisChannelTripEvents      = "studex:trip:events"
	RedisChannelRatingEvents    = "studex:rating:events"
	RedisChannelGpsStatus       = "studex:gps:status"
	RedisChannelDriverStatus    = "studex:driver:status"
)

type MatchCompletedData struct {
	RequestID      string             `json:"request_id"`
	OrderID        string             `json:"order_id"`
	DriverRefID    string             `json:"driver_ref_id"`
	CustomerRefID  string             `json:"customer_ref_id"`
	Score          float64            `json:"score"`
	Factors        map[string]float64 `json:"factors"`
	PickupLat      float64            `json:"pickup_lat"`
	PickupLng      float64            `json:"pickup_lng"`
	DestLat        float64            `json:"dest_lat"`
	DestLng        float64            `json:"dest_lng"`
	ServiceType    string             `json:"service_type"`
	EstimatedPrice float64            `json:"estimated_price"`
}

type MatchTimeoutData struct {
	RequestID     string `json:"request_id"`
	OrderID       string `json:"order_id"`
	CustomerRefID string `json:"customer_ref_id"`
}

type MatchCancelledData struct {
	RequestID     string `json:"request_id"`
	OrderID       string `json:"order_id"`
	CustomerRefID string `json:"customer_ref_id"`
}

type TripCreatedData struct {
	TripID        string `json:"trip_id"`
	OrderID       string `json:"order_id"`
	DriverRefID   string `json:"driver_ref_id"`
	CustomerRefID string `json:"customer_ref_id"`
	Status        string `json:"status"`
}

type TripStartedData struct {
	TripID        string `json:"trip_id"`
	OrderID       string `json:"order_id"`
	DriverRefID   string `json:"driver_ref_id"`
	CustomerRefID string `json:"customer_ref_id"`
}

type TripCompletedData struct {
	TripID        string  `json:"trip_id"`
	OrderID       string  `json:"order_id"`
	DriverRefID   string  `json:"driver_ref_id"`
	CustomerRefID string  `json:"customer_ref_id"`
	FinalPrice    float64 `json:"final_price"`
	PaymentStatus string  `json:"payment_status"`
	DebtAmount    float64 `json:"debt_amount,omitempty"`
}

type TripCancelledData struct {
	TripID        string `json:"trip_id"`
	OrderID       string `json:"order_id"`
	DriverRefID   string `json:"driver_ref_id"`
	CustomerRefID string `json:"customer_ref_id"`
	Reason        string `json:"reason"`
}

type LocationUpdatedData struct {
	RefID     string  `json:"ref_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"`
}

type GpsStatusData struct {
	RefID     string `json:"ref_id"`
	GpsActive bool   `json:"gps_active"`
}

type TripBargainingData struct {
	TripID          string  `json:"trip_id"`
	OrderID         string  `json:"order_id"`
	DriverRefID     string  `json:"driver_ref_id"`
	CustomerRefID   string  `json:"customer_ref_id"`
	CurrentBidPrice float64 `json:"current_bid_price"`
	LastBidder      string  `json:"last_bidder"`
}

type TripDealData struct {
	TripID        string  `json:"trip_id"`
	OrderID       string  `json:"order_id"`
	DriverRefID   string  `json:"driver_ref_id"`
	CustomerRefID string  `json:"customer_ref_id"`
	FinalPrice    float64 `json:"final_price"`
}

type TripAbortedData struct {
	TripID        string `json:"trip_id"`
	OrderID       string `json:"order_id"`
	DriverRefID   string `json:"driver_ref_id"`
	CustomerRefID string `json:"customer_ref_id"`
	Reason        string `json:"reason"`
	AbortedBy     string `json:"aborted_by"`
}

type RatingSubmittedData struct {
	RatingID  string `json:"rating_id"`
	OrderID   string `json:"order_id"`
	TripID    string `json:"trip_id"`
	RaterType string `json:"rater_type"`
	RaterID   string `json:"rater_id"`
	RateeID   string `json:"ratee_id"`
	Score     int    `json:"score"`
	Status    string `json:"status"`
}

type RatingConfirmedData struct {
	RatingID string `json:"rating_id"`
	RateeID  string `json:"ratee_id"`
	Score    int    `json:"score"`
}

type RatingRejectedData struct {
	RatingID string `json:"rating_id"`
	RateeID  string `json:"ratee_id"`
}

type DriverStatusChangedData struct {
	DriverRefID string `json:"driver_ref_id"`
	Phone       string `json:"phone"`
	Gender      string `json:"gender,omitempty"`
	IsOnline    bool   `json:"is_online"`
}

type RematchRequestedData struct {
	TripID           string   `json:"trip_id"`
	OrderID          string   `json:"order_id"`
	CustomerID       string   `json:"customer_id"`
	PickupLat         float64  `json:"pickup_lat"`
	PickupLng         float64  `json:"pickup_lng"`
	DestLat           float64  `json:"dest_lat"`
	DestLng           float64  `json:"dest_lng"`
	ExcludedDrivers   []string `json:"excluded_drivers"`
	Attempt           int      `json:"attempt"`
	MaxAttempts       int      `json:"max_attempts"`
}
