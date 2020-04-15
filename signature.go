package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func generateSignature(key, uid, channel string) string {

	signArray := []string{uid, channel}
	sign := strings.Join(signArray, ":")

	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(sign))

	//return base64.StdEncoding.EncodeToString(h.Sum(nil))
	return hex.EncodeToString(h.Sum(nil))
}

func generatePresenceSignature(key, uid, channel string, data []byte) string {

	signArray := []string{uid, channel, string(data)}
	sign := strings.Join(signArray, ":")

	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(sign))

	return hex.EncodeToString(h.Sum(nil))
}

func verifySignature(generated, sign string) bool {

	return hmac.Equal([]byte(generated), []byte(sign))

}
