package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"strings"
)

func generateSignature(key, uid, channel string) string {
	var hash []byte

	signArray := []string{key, uid, channel}
	sign := strings.Join(signArray, ":")

	hmac.New(sha256.New, []byte(sign)).Write(hash)

	return string(hash)
}

func verifySignature(generated, sign string) bool {

	return hmac.Equal([]byte(generated), []byte(sign))

}
