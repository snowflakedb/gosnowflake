// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"fmt"
)

const HeaderSnowflakeToken = "Snowflake Token=\"%v\""
const HeaderAuthorizationKey = "Authorization"

const ContentTypeApplicationJson = "application/json"
const AcceptTypeAppliationSnowflake = "application/snowflake"
const AcceptTypeAppliationJson = ContentTypeApplicationJson

const ClientType = "Go"
const ClientVersion = "0.1" // TODO: should be updated at build time
const OSVersion = "0.11" // TODO: should be retrieved

var UserAgent string = fmt.Sprintf("%v %v", ClientType, ClientVersion)

const StatementTypeIdDml = int64(0x3000)
const StatementTypeIdInsert = StatementTypeIdDml + int64(0x100)
const StatementTypeIdUpdate = StatementTypeIdDml + int64(0x200)
const StatementTypeIdDelete = StatementTypeIdDml + int64(0x300)
const StatementTypeIdMerge = StatementTypeIdDml + int64(0x400)
const StatementTypeIdMultiTableInsert = StatementTypeIdDml + int64(0x500)

const HeaderSseCAlgorithm = "x-amz-server-side-encryption-customer-algorithm"
const HeaderSseCKey = "x-amz-server-side-encryption-customer-key"
const HeaderSseCAes = "AES256"
