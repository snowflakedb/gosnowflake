package gosnowflake

func testEncryptionMeta() *encryptMetadata {
	const mockMatDesc = "{\"queryid\":\"01abc874-0406-1bf0-0000-53b10668e056\",\"smkid\":\"92019681909886\",\"key\":\"128\"}"
	return &encryptMetadata{
		key:     "testencryptedkey12345678910==",
		iv:      "testIVkey12345678910==",
		matdesc: mockMatDesc,
	}
}
