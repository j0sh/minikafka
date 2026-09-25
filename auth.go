package minikafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
	scram "github.com/xdg-go/scram"
	"github.com/xdg-go/stringprep"
)

const (
	kerrUnsupportedSASLMechanism int16 = 33
	kerrSASLAuthenticationFailed int16 = 58
	scramIterations                    = 4096
	maxSASLTokenBytes                  = 1 << 20
	maxSASLRequestBytes                = maxSASLTokenBytes + 4096
	saslAuthenticationTimeout          = 10 * time.Second
)

type brokerAuth struct {
	mechanisms []SASLMechanism
	users      map[string]string
	scramUsers map[string]string
	scram      *scram.Server
}

func newBrokerAuth(cfg *SASLConfig) (*brokerAuth, error) {
	if cfg == nil {
		return nil, nil
	}
	mechanisms := cfg.Mechanisms
	if len(mechanisms) == 0 {
		mechanisms = []SASLMechanism{SASLSCRAMSHA512}
	}
	var plainEnabled, scramEnabled bool
	for _, mechanism := range mechanisms {
		switch mechanism {
		case SASLPlain:
			if plainEnabled {
				return nil, fmt.Errorf("%w: duplicate mechanism %q", ErrInvalidSASLConfig, mechanism)
			}
			plainEnabled = true
		case SASLSCRAMSHA512:
			if scramEnabled {
				return nil, fmt.Errorf("%w: duplicate mechanism %q", ErrInvalidSASLConfig, mechanism)
			}
			scramEnabled = true
		default:
			return nil, fmt.Errorf("%w: unsupported mechanism %q", ErrInvalidSASLConfig, mechanism)
		}
	}
	if len(cfg.Users) == 0 {
		return nil, fmt.Errorf("%w: at least one user is required", ErrInvalidSASLConfig)
	}
	auth := &brokerAuth{mechanisms: slices.Clone(mechanisms)}
	if plainEnabled {
		auth.users = make(map[string]string, len(cfg.Users))
	}
	if scramEnabled {
		credentials := make(map[string]scram.StoredCredentials, len(cfg.Users))
		auth.scramUsers = make(map[string]string, len(cfg.Users))
		for user, password := range cfg.Users {
			if err := validateSASLUser(user, password); err != nil {
				return nil, err
			}
			if plainEnabled {
				auth.users[user] = password
			}
			preparedUser, err := stringprep.SASLprep.Prepare(user)
			if err != nil || preparedUser == "" {
				return nil, fmt.Errorf("%w: invalid SCRAM username %q", ErrInvalidSASLConfig, user)
			}
			if _, exists := credentials[preparedUser]; exists {
				return nil, fmt.Errorf("%w: duplicate normalized SCRAM username %q", ErrInvalidSASLConfig, user)
			}
			client, err := scram.SHA512.NewClient(user, password, "")
			if err != nil {
				return nil, fmt.Errorf("%w: invalid SCRAM credentials for %q", ErrInvalidSASLConfig, user)
			}
			salt := make([]byte, 24)
			if _, err := rand.Read(salt); err != nil {
				return nil, fmt.Errorf("%w: generate SCRAM salt: %v", ErrInvalidSASLConfig, err)
			}
			credential, err := client.GetStoredCredentialsWithError(scram.KeyFactors{Salt: string(salt), Iters: scramIterations})
			if err != nil {
				return nil, fmt.Errorf("%w: derive SCRAM credentials for %q", ErrInvalidSASLConfig, user)
			}
			credentials[preparedUser] = credential
			auth.scramUsers[preparedUser] = user
		}
		server, err := scram.SHA512.NewServer(func(user string) (scram.StoredCredentials, error) {
			preparedUser, err := decodeSCRAMName(user)
			if err == nil {
				preparedUser, err = stringprep.SASLprep.Prepare(preparedUser)
			}
			if err != nil {
				return scram.StoredCredentials{}, errors.New("invalid username")
			}
			credential, ok := credentials[preparedUser]
			if !ok {
				return scram.StoredCredentials{}, errors.New("unknown user")
			}
			return credential, nil
		})
		if err != nil {
			return nil, fmt.Errorf("%w: initialize SCRAM server: %v", ErrInvalidSASLConfig, err)
		}
		auth.scram = server
		return auth, nil
	}
	for user, password := range cfg.Users {
		if err := validateSASLUser(user, password); err != nil {
			return nil, err
		}
		auth.users[user] = password
	}
	return auth, nil
}

func (a *brokerAuth) supports(mechanism SASLMechanism) bool {
	return slices.Contains(a.mechanisms, mechanism)
}

func (a *brokerAuth) mechanismNames() []string {
	names := make([]string, len(a.mechanisms))
	for i, mechanism := range a.mechanisms {
		names[i] = string(mechanism)
	}
	return names
}

func validateSASLUser(user, password string) error {
	if user == "" || password == "" || strings.ContainsRune(user, 0) || strings.ContainsRune(password, 0) {
		return fmt.Errorf("%w: users and passwords must be non-empty and contain no NUL bytes", ErrInvalidSASLConfig)
	}
	return nil
}

// decodeSCRAMName undoes the SASL name escapes used in SCRAM messages.
func decodeSCRAMName(encoded string) (string, error) {
	var decoded strings.Builder
	for i := 0; i < len(encoded); i++ {
		if encoded[i] != '=' {
			decoded.WriteByte(encoded[i])
			continue
		}
		if len(encoded)-i < 3 {
			return "", errors.New("invalid SCRAM name escape")
		}
		switch encoded[i+1 : i+3] {
		case "2C":
			decoded.WriteByte(',')
		case "3D":
			decoded.WriteByte('=')
		default:
			return "", errors.New("invalid SCRAM name escape")
		}
		i += 2
	}
	return decoded.String(), nil
}

// authenticateConn handles the Kafka requests allowed before authentication.
func (b *Broker) authenticateConn(ctx context.Context, conn net.Conn) (string, bool) {
	if err := conn.SetDeadline(time.Now().Add(saslAuthenticationTimeout)); err != nil {
		return "", false
	}
	defer conn.SetDeadline(time.Time{})
	for {
		version, correlationID, msg, err := readAuthRequest(conn)
		if err != nil {
			return "", false
		}
		switch req := msg.(type) {
		case *apiversions.Request:
			if version > 2 {
				// New clients can probe with a version we do not implement.
				// Kafka sends a v0 error response with the supported range.
				if err := protocol.WriteResponse(conn, 0, correlationID, &apiversions.Response{
					ErrorCode: kerrUnsupportedVersion,
					ApiKeys: []apiversions.ApiKeyResponse{{
						ApiKey: int16(protocol.ApiVersions), MinVersion: 0, MaxVersion: 2,
					}},
				}); err != nil {
					return "", false
				}
				continue
			}
			if err := protocol.WriteResponse(conn, version, correlationID, b.handle(ctx, version, "", req)); err != nil {
				return "", false
			}
		case *saslhandshake.Request:
			mechanism := SASLMechanism(req.Mechanism)
			if !b.auth.supports(mechanism) {
				_ = protocol.WriteResponse(conn, version, correlationID, &saslhandshake.Response{
					ErrorCode:  kerrUnsupportedSASLMechanism,
					Mechanisms: b.auth.mechanismNames(),
				})
				return "", false
			}
			if err := protocol.WriteResponse(conn, version, correlationID, &saslhandshake.Response{
				Mechanisms: b.auth.mechanismNames(),
			}); err != nil {
				return "", false
			}
			if version == 0 {
				return b.authenticateRaw(conn, mechanism)
			}
			return b.authenticateFramed(conn, mechanism)
		default:
			return "", false
		}
	}
}

type saslConversation struct {
	auth      *brokerAuth
	mechanism SASLMechanism
	scram     *scram.ServerConversation
	user      string
}

func (a *brokerAuth) newConversation(mechanism SASLMechanism) saslConversation {
	c := saslConversation{auth: a, mechanism: mechanism}
	if mechanism == SASLSCRAMSHA512 {
		c.scram = a.scram.NewConversation()
	}
	return c
}

func (c *saslConversation) step(token []byte) (response []byte, done bool, err error) {
	if len(token) > maxSASLTokenBytes {
		return nil, false, errors.New("SASL token too large")
	}
	if c.mechanism == SASLPlain {
		parts := bytes.SplitN(token, []byte{0}, 4)
		if len(parts) != 3 || len(parts[1]) == 0 || len(parts[2]) == 0 {
			return nil, false, errors.New("malformed PLAIN token")
		}
		user := string(parts[1])
		if len(parts[0]) > 0 && !bytes.Equal(parts[0], parts[1]) {
			return nil, false, errors.New("authorization identity differs from username")
		}
		password, ok := c.auth.users[user]
		if !ok || subtle.ConstantTimeCompare([]byte(password), parts[2]) != 1 {
			return nil, false, errors.New("invalid credentials")
		}
		c.user = user
		return nil, true, nil
	}
	responseString, err := c.scram.Step(string(token))
	if err != nil {
		return nil, false, err
	}
	user, err := decodeSCRAMName(c.scram.Username())
	if err != nil {
		return nil, false, err
	}
	if authzID := c.scram.AuthzID(); authzID != "" {
		decodedAuthzID, err := decodeSCRAMName(authzID)
		if err != nil || decodedAuthzID != user {
			return nil, false, errors.New("authorization identity differs from username")
		}
	}
	if c.scram.Done() && c.scram.Valid() {
		prepared, err := stringprep.SASLprep.Prepare(user)
		if err != nil {
			return nil, false, err
		}
		c.user = c.auth.scramUsers[prepared]
		if c.user == "" {
			return nil, false, errors.New("unknown user")
		}
		return []byte(responseString), true, nil
	}
	return []byte(responseString), false, nil
}

func (b *Broker) authenticateFramed(conn net.Conn, mechanism SASLMechanism) (string, bool) {
	conversation := b.auth.newConversation(mechanism)
	for {
		version, correlationID, msg, err := readAuthRequest(conn)
		if err != nil {
			return "", false
		}
		req, ok := msg.(*saslauthenticate.Request)
		if !ok {
			return "", false
		}
		response, done, err := conversation.step(req.AuthBytes)
		res := &saslauthenticate.Response{AuthBytes: response}
		if err != nil {
			res.ErrorCode = kerrSASLAuthenticationFailed
			res.ErrorMessage = "SASL authentication failed"
		}
		if writeErr := protocol.WriteResponse(conn, version, correlationID, res); writeErr != nil || err != nil {
			return "", false
		}
		if done {
			return conversation.user, true
		}
	}
}

func (b *Broker) authenticateRaw(conn net.Conn, mechanism SASLMechanism) (string, bool) {
	conversation := b.auth.newConversation(mechanism)
	for {
		var length [4]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return "", false
		}
		size := binary.BigEndian.Uint32(length[:])
		if size > maxSASLTokenBytes {
			return "", false
		}
		token := make([]byte, size)
		if _, err := io.ReadFull(conn, token); err != nil {
			return "", false
		}
		response, done, err := conversation.step(token)
		if err != nil {
			return "", false
		}
		binary.BigEndian.PutUint32(length[:], uint32(len(response)))
		if err := writeAll(conn, length[:]); err != nil {
			return "", false
		}
		if err := writeAll(conn, response); err != nil {
			return "", false
		}
		if done {
			return conversation.user, true
		}
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// Parse only the three non-flexible request types accepted before authentication.
// The generic protocol decoder allocates from client-provided field lengths, so
// unauthenticated frames and their fields must be bounded before decoding.
func readAuthRequest(conn net.Conn) (int16, int32, protocol.Message, error) {
	var length [4]byte
	if _, err := io.ReadFull(conn, length[:]); err != nil {
		return 0, 0, nil, err
	}
	size := binary.BigEndian.Uint32(length[:])
	if size < 10 || size > maxSASLRequestBytes {
		return 0, 0, nil, errors.New("invalid SASL request size")
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(conn, frame); err != nil {
		return 0, 0, nil, err
	}
	key := protocol.ApiKey(binary.BigEndian.Uint16(frame[:2]))
	version := int16(binary.BigEndian.Uint16(frame[2:4]))
	correlationID := int32(binary.BigEndian.Uint32(frame[4:8]))
	clientIDLength := int(int16(binary.BigEndian.Uint16(frame[8:10])))
	if clientIDLength < -1 || 10+max(clientIDLength, 0) > len(frame) {
		return 0, 0, nil, errors.New("invalid SASL request client ID")
	}
	body := frame[10+max(clientIDLength, 0):]
	switch key {
	case protocol.ApiVersions:
		if version < 0 || (version <= 2 && len(body) != 0) {
			break
		}
		return version, correlationID, &apiversions.Request{}, nil
	case protocol.SaslHandshake:
		if (version != 0 && version != 1) || len(body) < 2 {
			break
		}
		mechanismLength := int(int16(binary.BigEndian.Uint16(body[:2])))
		if mechanismLength < 0 || mechanismLength != len(body)-2 {
			break
		}
		return version, correlationID, &saslhandshake.Request{Mechanism: string(body[2:])}, nil
	case protocol.SaslAuthenticate:
		if (version != 0 && version != 1) || len(body) < 4 {
			break
		}
		tokenLength := int(int32(binary.BigEndian.Uint32(body[:4])))
		if tokenLength < 0 || tokenLength > maxSASLTokenBytes || tokenLength != len(body)-4 {
			break
		}
		return version, correlationID, &saslauthenticate.Request{AuthBytes: body[4:]}, nil
	}
	return 0, 0, nil, errors.New("invalid or disallowed SASL request")
}
