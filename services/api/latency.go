package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/flashbots/go-utils/cli"
	"github.com/flashbots/mev-boost-relay/common"
)

var (
	latencyRequestTimeout = time.Duration(cli.GetEnvInt("LATENCY_TIMEOUT_MS", 50)) * time.Millisecond
)

var (
	defaultClientRttMs = cli.GetEnvInt("DEFAULT_CLIENT_RTT_MS", 300)
	rttToHandshakeScale = common.GetEnvFloat("RTT_TO_HANDSHAKE_SCALE", 1.5)
	rttToResponseScale = common.GetEnvFloat("RTT_TO_RESPONSE_SCALE", 0.5)
)

type LatencyRequest struct {
	IP string `json:"ip"`
}

type LatencyResponse struct {
	IP        string    `json:"ip"`
	Port      int64     `json:"port"`
	RttMin    int64     `json:"rtt_min"`
	RttAv     int64     `json:"rtt_av"`
	Method    string    `json:"method"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type LatencyEstimator struct {
	latencyServiceURI string
	client *http.Client
}

func NewLatencyEstimator(latencyServiceURI string) *LatencyEstimator {
	return &LatencyEstimator{
		latencyServiceURI: latencyServiceURI,
		client: &http.Client{ //nolint:exhaustivestruct
			Timeout: latencyRequestTimeout,
		},
	}
}

// GetRtt returns the round-trip time response struct to the given IP address
// by querying the latency service
func (le *LatencyEstimator) GetRtt(ip string) (respData LatencyResponse, err error) {
	reqData := &LatencyRequest{IP: ip}
	reqBytes, err := json.Marshal(reqData)
	if err != nil {
		return respData, err
	}

	start := time.Now()
	req, err := http.NewRequest("GET", le.latencyServiceURI, bytes.NewReader(reqBytes))
	if err != nil {
		return respData, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := le.client.Do(req)
	if err != nil {
		return respData, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return respData, errors.New(fmt.Sprintf("latency service returned status code %d", resp.StatusCode))
	}

	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return respData, err
	}
	fmt.Printf("TGAAS: latency service request took %v\n", time.Since(start))
	return respData, nil
}

// Extracts the client IP address from a request in the format needed by the latency service
func (le *LatencyEstimator) GetClientIP(r *http.Request) (clientIP string, err error) {
	clientIP = r.Header.Get("X-Real-IP")
	if clientIP == "" {
		forwarded := r.Header.Get("X-Forwarded-For")
		if forwarded != "" {
			clientIP = strings.Split(forwarded, ",")[0]
		}
	}
	if clientIP == "" {
		IPPort := r.RemoteAddr
		clientIP, _, err = net.SplitHostPort(IPPort)
		if err != nil {
			return "", err
		}
	}
	fmt.Printf("TGAAS: real-ip=%s, forwarded-for=%s, remote-addr=%s\n", r.Header.Get("X-Real-IP"), r.Header.Get("X-Forwarded-For"), r.RemoteAddr)
	return clientIP, nil
}

// Estimates the elapsed time since the client intiated the request
// and the time needed for response to be sent back to the client
// maxElapsedMs is a known limit: the call could not have been initated more than maxElapsedMs ago
// Errors are not fatal but returned for logging; a viable usable default value is always returned
func (le *LatencyEstimator) EstimateTiming(r *http.Request, maxElapsedMs uint64) (elapsedMs, responseMs uint64, err error) {
	start := time.Now()
	rtt := uint64(defaultClientRttMs)
	clientIP, err := le.GetClientIP(r)
	var respData LatencyResponse
	if err == nil {
		respData, err = le.GetRtt(clientIP)
		if err == nil && respData.RttAv >= 0 {
			rtt = uint64(respData.RttAv)
		} else if err != nil {
			fmt.Printf("TGAAS: failed to get RTT for clientIP=%s: %v\n", clientIP, err)
		}
	}

	responseMs = uint64(float64(rtt) * rttToResponseScale)

	// Estimate elapsed time from latency service first, cap at maxElapsedMs, and add time spent in this fn
	elapsedMs = uint64(float64(rtt) * rttToHandshakeScale)
	if elapsedMs > maxElapsedMs {
		elapsedMs = maxElapsedMs
	}
	elapsedMs += uint64(time.Since(start).Milliseconds())
	fmt.Printf("TGAAS: clientIP=%s, RttAv=%d rtt=%d, elapsedMs=%d, responseMs=%d\n", clientIP, respData.RttAv, rtt, elapsedMs, responseMs)
	return elapsedMs, responseMs, err
}
