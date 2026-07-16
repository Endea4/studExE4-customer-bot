package pricing

import "math"

type PriceConfig struct {
	BaseFare       float64
	PerKmRate      float64
	MinFare        float64
	ServiceFeeRate float64
	AvgSpeedKmH    float64
}

func DefaultConfig() PriceConfig {
	return PriceConfig{
		BaseFare:       5000,
		PerKmRate:      2000,
		MinFare:        8000,
		ServiceFeeRate: 0.10,
		AvgSpeedKmH:    25.0,
	}
}

var ServiceConfigs = map[string]PriceConfig{
	"anjem": {
		BaseFare:       5000,
		PerKmRate:      2000,
		MinFare:        8000,
		ServiceFeeRate: 0.10,
		AvgSpeedKmH:    25.0,
	},
	"jastip": {
		BaseFare:       7000,
		PerKmRate:      2500,
		MinFare:        10000,
		ServiceFeeRate: 0.10,
		AvgSpeedKmH:    20.0,
	},
}

func GetConfig(serviceType string) PriceConfig {
	if cfg, ok := ServiceConfigs[serviceType]; ok {
		return cfg
	}
	return DefaultConfig()
}

func CalculateDistance(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLng := (lng2 - lng1) * math.Pi / 180.0
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180.0)*math.Cos(lat2*math.Pi/180.0)*
			math.Sin(dLng/2)*math.Sin(dLng/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func CalculatePrice(cfg PriceConfig, distanceKm float64) PriceBreakdown {
	distanceKm = math.Max(distanceKm, 0)
	subtotal := cfg.BaseFare + (distanceKm * cfg.PerKmRate)
	if subtotal < cfg.MinFare {
		subtotal = cfg.MinFare
	}
	serviceFee := math.Round(subtotal * cfg.ServiceFeeRate)
	total := subtotal + serviceFee

	return PriceBreakdown{
		BaseFare:       cfg.BaseFare,
		DistanceKm:    math.Round(distanceKm*100) / 100,
		PerKmRate:      cfg.PerKmRate,
		Subtotal:       math.Round(subtotal),
		ServiceFee:     math.Round(serviceFee),
		Total:          math.Round(total),
		DriverPayout:   math.Round(subtotal),
		EstimatedMin:   ETA(cfg, distanceKm),
	}
}

func ETA(cfg PriceConfig, distanceKm float64) int {
	if cfg.AvgSpeedKmH <= 0 {
		cfg.AvgSpeedKmH = 25.0
	}
	minutes := (distanceKm / cfg.AvgSpeedKmH) * 60
	return int(math.Max(math.Round(minutes), 1))
}

type PriceBreakdown struct {
	BaseFare     float64 `json:"base_fare"`
	DistanceKm   float64 `json:"distance_km"`
	PerKmRate    float64 `json:"per_km_rate"`
	Subtotal     float64 `json:"subtotal"`
	ServiceFee   float64 `json:"service_fee"`
	Total        float64 `json:"total"`
	DriverPayout float64 `json:"driver_payout"`
	EstimatedMin int     `json:"estimated_min"`
}
