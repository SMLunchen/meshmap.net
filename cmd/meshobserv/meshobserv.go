package main

import (
	"bufio"
	"errors"
	"flag"
	"io/fs"
	"log"
	"math"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/brianshea2/meshmap.net/internal/meshtastic"
	"github.com/brianshea2/meshmap.net/internal/meshtastic/generated"
	"google.golang.org/protobuf/proto"
)

const (
	NodeExpiration     = 86400 // 24 hr
	NeighborExpiration = 28000 // ~7.8 hr
	MetricsExpiration  = 28800 // 8 hr
	PruneWriteInterval = time.Minute
	RateLimitCount     = 1500
	RateLimitDuration  = 5 * time.Minute
)

var (
	Nodes      meshtastic.NodeDB
	NodesMutex sync.Mutex
	Receiving  atomic.Bool
)

// recordTracerouteHops records directed RF edges from a traceroute path segment list.
// The full chain is origin, route..., dest; snr[i] is the SNR at which chain[i+1]
// received chain[i], scaled by 4; math.MinInt8 means unknown. A final snr entry beyond
// route means dest itself completed the reception. Only already-known nodes are updated.
func recordTracerouteHops(origin, dest uint32, route []uint32, snr []int32) {
	chain := make([]uint32, 0, len(route)+2)
	chain = append(chain, origin)
	chain = append(chain, route...)
	if len(snr) == len(route)+1 && dest != 0 && dest != 0xffffffff {
		chain = append(chain, dest)
	}
	NodesMutex.Lock()
	for i := 0; i < len(chain)-1 && i < len(snr); i++ {
		transmitter, receiver := chain[i], chain[i+1]
		if transmitter == 0 || transmitter == 0xffffffff || receiver == 0 || transmitter == receiver {
			continue
		}
		// rx_snr of exactly 0 means the hop crossed MQTT (downlink), not RF
		if snr[i] == 0 {
			continue
		}
		node := Nodes[receiver]
		if node == nil {
			continue
		}
		info := &meshtastic.NeighborInfo{Source: meshtastic.NeighborSourceTraceroute}
		if snr[i] != math.MinInt8 {
			s := float32(snr[i]) / 4
			info.Snr = &s
		}
		node.UpdateNeighbor(transmitter, info)
	}
	NodesMutex.Unlock()
}

func handleMessage(from uint32, topic string, portNum generated.PortNum, payload []byte, info *meshtastic.PacketInfo) {
	Receiving.Store(true)
	// a packet with hop_start == hop_limit was received by the uplinking gateway
	// directly over RF (zero hops): record it as a measured RF link at the gateway;
	// via_mqtt packets are rebroadcasts of MQTT-downlinked traffic, where the RF
	// measurement belongs to the downlinking gateway, not the original sender
	if info != nil && info.GatewayId != 0 && info.GatewayId != from && !info.ViaMqtt &&
		info.HopStart > 0 && info.HopStart == info.HopLimit &&
		(info.RxSnr != 0 || info.RxRssi != 0) {
		NodesMutex.Lock()
		if gw := Nodes[info.GatewayId]; gw != nil {
			snr := info.RxSnr
			gw.UpdateNeighbor(from, &meshtastic.NeighborInfo{
				Snr:    &snr,
				Rssi:   info.RxRssi,
				Source: meshtastic.NeighborSourceGateway,
			})
		}
		NodesMutex.Unlock()
	}
	switch portNum {
	case generated.PortNum_TEXT_MESSAGE_APP:
		log.Printf("[msg] (%v) <%v> %s", topic, from, payload)
	case generated.PortNum_POSITION_APP:
		var position generated.Position
		if err := proto.Unmarshal(payload, &position); err != nil {
			log.Printf("[warn] could not parse Position payload from %v on %v: %v", from, topic, err)
			return
		}
		latitude := position.GetLatitudeI()
		longitude := position.GetLongitudeI()
		altitude := position.GetAltitude()
		precision := position.GetPrecisionBits()
		if !meshtastic.ValidPosition(latitude, longitude) {
			return
		}
		NodesMutex.Lock()
		if Nodes[from] == nil {
			Nodes[from] = meshtastic.NewNode(topic)
		}
		Nodes[from].UpdatePosition(latitude, longitude, altitude, precision)
		Nodes[from].UpdateSeenBy(topic)
		NodesMutex.Unlock()
	case generated.PortNum_NODEINFO_APP:
		var user generated.User
		if err := proto.Unmarshal(payload, &user); err != nil {
			log.Printf("[warn] could not parse User payload from %v on %v: %v", from, topic, err)
			return
		}
		longName := user.GetLongName()
		shortName := user.GetShortName()
		hwModel := user.GetHwModel().String()
		role := user.GetRole().String()
		if len(longName) == 0 {
			return
		}
		NodesMutex.Lock()
		if Nodes[from] == nil {
			Nodes[from] = meshtastic.NewNode(topic)
		}
		Nodes[from].UpdateUser(longName, shortName, hwModel, role)
		NodesMutex.Unlock()
	case generated.PortNum_TELEMETRY_APP:
		var telemetry generated.Telemetry
		if err := proto.Unmarshal(payload, &telemetry); err != nil {
			log.Printf("[warn] could not parse Telemetry payload from %v on %v: %v", from, topic, err)
			return
		}
		if deviceMetrics := telemetry.GetDeviceMetrics(); deviceMetrics != nil {
			batteryLevel := deviceMetrics.GetBatteryLevel()
			voltage := deviceMetrics.GetVoltage()
			chUtil := deviceMetrics.GetChannelUtilization()
			airUtilTx := deviceMetrics.GetAirUtilTx()
			uptime := deviceMetrics.GetUptimeSeconds()
			NodesMutex.Lock()
			if Nodes[from] == nil {
				Nodes[from] = meshtastic.NewNode(topic)
			}
			Nodes[from].UpdateDeviceMetrics(batteryLevel, voltage, chUtil, airUtilTx, uptime)
			NodesMutex.Unlock()
		} else if envMetrics := telemetry.GetEnvironmentMetrics(); envMetrics != nil {
			temperature := envMetrics.GetTemperature()
			relativeHumidity := envMetrics.GetRelativeHumidity()
			barometricPressure := envMetrics.GetBarometricPressure()
			lux := envMetrics.GetLux()
			windDirection := envMetrics.GetWindDirection()
			windSpeed := envMetrics.GetWindSpeed()
			windGust := envMetrics.GetWindGust()
			radiation := envMetrics.GetRadiation()
			rainfall1 := envMetrics.GetRainfall_1H()
			rainfall24 := envMetrics.GetRainfall_24H()
			NodesMutex.Lock()
			if Nodes[from] == nil {
				Nodes[from] = meshtastic.NewNode(topic)
			}
			Nodes[from].UpdateEnvironmentMetrics(
				temperature,
				relativeHumidity,
				barometricPressure,
				lux,
				windDirection,
				windSpeed,
				windGust,
				radiation,
				rainfall1,
				rainfall24,
			)
			NodesMutex.Unlock()
		}
	case generated.PortNum_NEIGHBORINFO_APP:
		var neighborInfo generated.NeighborInfo
		if err := proto.Unmarshal(payload, &neighborInfo); err != nil {
			log.Printf("[warn] could not parse NeighborInfo payload from %v on %v: %v", from, topic, err)
			return
		}
		nodeNum := neighborInfo.GetNodeId()
		neighbors := neighborInfo.GetNeighbors()
		if nodeNum != from {
			return
		}
		if len(neighbors) == 0 {
			return
		}
		NodesMutex.Lock()
		if Nodes[from] == nil {
			Nodes[from] = meshtastic.NewNode(topic)
		}
		for _, neighbor := range neighbors {
			neighborNum := neighbor.GetNodeId()
			if neighborNum == 0 {
				continue
			}
			snr := neighbor.GetSnr()
			// exact 0 is the fingerprint of an MQTT-learned "neighbor"
			// (older firmware records MQTT-downlinked nodes with rx_snr 0)
			if snr == 0 {
				continue
			}
			Nodes[from].UpdateNeighbor(neighborNum, &meshtastic.NeighborInfo{
				Snr:    &snr,
				Source: meshtastic.NeighborSourceNeighborInfo,
			})
		}
		NodesMutex.Unlock()
	case generated.PortNum_TRACEROUTE_APP:
		var routeDiscovery generated.RouteDiscovery
		if err := proto.Unmarshal(payload, &routeDiscovery); err != nil {
			log.Printf("[warn] could not parse RouteDiscovery payload from %v on %v: %v", from, topic, err)
			return
		}
		if info == nil {
			return
		}
		// requests travel requester->destination collecting route/snr_towards;
		// replies (request_id set) carry the completed forward path and collect
		// route_back/snr_back on the way back to the requester
		isReply := info.RequestId != 0
		towardsOrigin, towardsDest := from, info.To
		if isReply {
			towardsOrigin, towardsDest = info.To, from
		}
		recordTracerouteHops(towardsOrigin, towardsDest, routeDiscovery.GetRoute(), routeDiscovery.GetSnrTowards())
		if isReply {
			recordTracerouteHops(from, info.To, routeDiscovery.GetRouteBack(), routeDiscovery.GetSnrBack())
		}
	case generated.PortNum_MAP_REPORT_APP:
		var mapReport generated.MapReport
		if err := proto.Unmarshal(payload, &mapReport); err != nil {
			log.Printf("[warn] could not parse MapReport payload from %v on %v: %v", from, topic, err)
			return
		}
		longName := mapReport.GetLongName()
		shortName := mapReport.GetShortName()
		hwModel := mapReport.GetHwModel().String()
		role := mapReport.GetRole().String()
		fwVersion := mapReport.GetFirmwareVersion()
		region := mapReport.GetRegion().String()
		modemPreset := mapReport.GetModemPreset().String()
		hasDefaultCh := mapReport.GetHasDefaultChannel()
		onlineLocalNodes := mapReport.GetNumOnlineLocalNodes()
		latitude := mapReport.GetLatitudeI()
		longitude := mapReport.GetLongitudeI()
		altitude := mapReport.GetAltitude()
		precision := mapReport.GetPositionPrecision()
		if len(longName) == 0 {
			return
		}
		if !meshtastic.ValidPosition(latitude, longitude) {
			return
		}
		NodesMutex.Lock()
		if Nodes[from] == nil {
			Nodes[from] = meshtastic.NewNode(topic)
		}
		Nodes[from].UpdateUser(longName, shortName, hwModel, role)
		Nodes[from].UpdateMapReport(fwVersion, region, modemPreset, hasDefaultCh, onlineLocalNodes)
		Nodes[from].UpdatePosition(latitude, longitude, altitude, precision)
		Nodes[from].UpdateSeenBy(topic)
		NodesMutex.Unlock()
	}
}

func main() {
	var dbPath, blockedPath string
	flag.StringVar(&dbPath, "f", "", "node database `file`")
	flag.StringVar(&blockedPath, "b", "", "node blocklist `file`")
	flag.Parse()
	// load or make NodeDB
	if len(dbPath) > 0 {
		err := Nodes.LoadFile(dbPath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Fatalf("[fatal] load nodes: %v", err)
		}
		log.Printf("[info] loaded %v nodes from disk", len(Nodes))
	}
	if Nodes == nil {
		Nodes = make(meshtastic.NodeDB)
	}
	// load node blocklist
	blocked := make(map[uint32]struct{})
	if len(blockedPath) > 0 {
		f, err := os.Open(blockedPath)
		if err != nil {
			log.Fatalf("[fatal] open blocklist: %v", err)
		}
		s := bufio.NewScanner(f)
		for s.Scan() {
			n, err := strconv.ParseUint(s.Text(), 10, 32)
			if err == nil {
				blocked[uint32(n)] = struct{}{}
				log.Printf("[info] node %v blocked", n)
			}
		}
		f.Close()
		err = s.Err()
		if err != nil {
			log.Fatalf("[fatal] read blocklist: %v", err)
		}
	}
	// maintain per-node message counters for rate limiting
	var counters sync.Map // as map[uint32]*uint32
	go func() {
		for {
			time.Sleep(RateLimitDuration)
			counters.Clear()
		}
	}()
	// connect to MQTT
	client := &meshtastic.MQTTClient{
		Topics: []string{
			"msh/+/2/map/",
			"msh/+/2/e/+/+",
			"msh/+/+/2/map/",
			"msh/+/+/2/e/+/+",
			"msh/+/+/+/2/map/",
			"msh/+/+/+/2/e/+/+",
			"msh/+/+/+/+/2/map/",
			"msh/+/+/+/+/2/e/+/+",
		},
		TopicRegex: regexp.MustCompile(`^msh(?:/[^/]+)+/2/(?:e/[^/]+/![0-9a-f]+|map/)$`),
		Accept: func(from uint32) bool {
			if _, found := blocked[from]; found {
				return false
			}
			v, _ := counters.LoadOrStore(from, new(uint32))
			count := atomic.AddUint32(v.(*uint32), 1)
			if count >= RateLimitCount {
				if count%100 == 0 {
					log.Printf("[info] node %v rate limited (%v messages)", from, count)
				}
				return false
			}
			return true
		},
		BlockCipher:    meshtastic.NewBlockCipher(meshtastic.DefaultKey),
		MessageHandler: handleMessage,
	}
	err := client.Connect()
	if err != nil {
		log.Fatalf("[fatal] connect: %v", err)
	}
	// start NodeDB prune and write loop
	go func() {
		for {
			time.Sleep(PruneWriteInterval)
			NodesMutex.Lock()
			Nodes.Prune(NodeExpiration, NeighborExpiration, MetricsExpiration, NodeExpiration)
			if len(dbPath) > 0 {
				valid := Nodes.GetValid()
				err := valid.WriteFile(dbPath)
				if err != nil {
					log.Fatalf("[fatal] write nodes: %v", err)
				}
				log.Printf("[info] wrote %v nodes to disk", len(valid))
			}
			NodesMutex.Unlock()
			if !Receiving.CompareAndSwap(true, false) {
				log.Fatal("[fatal] no messages received")
			}
		}
	}()
	// wait until exit
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)
	<-terminate
	log.Print("[info] exiting")
	client.Disconnect()
}
