package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func generateSignature(appId, uid, channel string) string {

	signArray := []string{uid, channel}
	sign := strings.Join(signArray, ":")

	h := hmac.New(sha256.New, []byte(appId))
	h.Write([]byte(sign))

	return hex.EncodeToString(h.Sum(nil))
}

func generatePresenceSignature(appId, uid, channel, id string, data []byte) string {

	signArray := []string{uid, channel, id, string(data)}
	sign := strings.Join(signArray, ":")

	h := hmac.New(sha256.New, []byte(appId))
	h.Write([]byte(sign))

	return hex.EncodeToString(h.Sum(nil))
}

func verifySignature(generated, sign string) bool {

	return hmac.Equal([]byte(generated), []byte(sign))

}
