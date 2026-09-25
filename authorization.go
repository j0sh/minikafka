package minikafka

import "fmt"

type topicPermissions uint8

const (
	permissionRead topicPermissions = 1 << iota
	permissionWrite
)

const wildcard = "*"

// authorizationPolicy is immutable after Open. The wildcard entries compose
// with specific entries, so any matching grant can authorize an action.
// Keys are user/topic pairs; nil disables authorization, an empty map denies all.
type authorizationPolicy map[[2]string]topicPermissions

func newAuthorizationPolicy(cfg *AuthorizationConfig, sasl *SASLConfig) (authorizationPolicy, error) {
	if cfg == nil {
		return nil, nil
	}
	if sasl == nil {
		return nil, fmt.Errorf("%w: SASL is required", ErrInvalidAuthorizationConfig)
	}
	if _, ok := sasl.Users[wildcard]; ok {
		return nil, fmt.Errorf("%w: %q is reserved as a user wildcard", ErrInvalidAuthorizationConfig, wildcard)
	}
	policy := make(authorizationPolicy, len(cfg.Grants))
	for _, grant := range cfg.Grants {
		if grant.User == "" || grant.Topic == "" {
			return nil, fmt.Errorf("%w: grant user and topic must be non-empty", ErrInvalidAuthorizationConfig)
		}
		if grant.User != wildcard {
			if _, ok := sasl.Users[grant.User]; !ok {
				return nil, fmt.Errorf("%w: unknown user %q", ErrInvalidAuthorizationConfig, grant.User)
			}
		}
		var permission topicPermissions
		switch grant.Action {
		case TopicRead:
			permission = permissionRead
		case TopicWrite:
			permission = permissionWrite
		case TopicAll:
			permission = permissionRead | permissionWrite
		default:
			return nil, fmt.Errorf("%w: unknown action %q", ErrInvalidAuthorizationConfig, grant.Action)
		}
		policy[[2]string{grant.User, grant.Topic}] |= permission
	}
	return policy, nil
}

func (p authorizationPolicy) allows(user, topic string, permission topicPermissions) bool {
	if p == nil {
		return true
	}
	for _, principal := range [...]string{user, wildcard} {
		for _, name := range [...]string{topic, wildcard} {
			if p[[2]string{principal, name}]&permission != 0 {
				return true
			}
		}
	}
	return false
}
