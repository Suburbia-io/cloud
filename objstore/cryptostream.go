package objstore

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"log"
)

func encryptReader(
	encKey []byte,
	rRaw io.Reader,
) (
	io.Reader,
	error,
) {
	block, err := aes.NewCipher(encKey)
	if err != nil {
		log.Printf("Failed to create AES block cipher: %v", err)
		return nil, err
	}

	iv := make([]byte, block.BlockSize())
	if _, err := rand.Read(iv); err != nil {
		log.Printf("Failed to create IV: %v", err)
		return nil, err
	}

	ivReader := bytes.NewBuffer(iv)

	sr := &cipher.StreamReader{
		S: cipher.NewCTR(block, iv),
		R: rRaw,
	}

	return io.MultiReader(ivReader, sr), nil
}

func decryptReader(
	encKey []byte,
	rRaw io.Reader,
) (
	io.Reader,
	error,
) {
	block, err := aes.NewCipher(encKey)
	if err != nil {
		log.Printf("Failed to create AES block cipher: %v", err)
		return nil, err
	}

	iv := make([]byte, block.BlockSize())
	if _, err := rRaw.Read(iv); err != nil {
		log.Printf("Failed to read IV: %v", err)
		return nil, err
	}

	return &cipher.StreamReader{
		S: cipher.NewCTR(block, iv),
		R: rRaw,
	}, nil
}
