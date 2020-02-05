package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func generateSignature(secret, key, uid, channel string) string {

	signArray := []string{key, uid, channel}
	sign := strings.Join(signArray, ":")

	salt := key + ":" + secret + ":" + uid

	h := hmac.New(sha256.New, []byte(salt))
	h.Write([]byte(sign))

	//return base64.StdEncoding.EncodeToString(h.Sum(nil))
	return hex.EncodeToString(h.Sum(nil))
}

func verifySignature(generated, sign string) bool {

	return hmac.Equal([]byte(generated), []byte(sign))

}
