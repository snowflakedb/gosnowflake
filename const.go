// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"fmt"
)

const HeaderSnowflakeToken = "Snowflake Token=\"%s\""
const HeaderAuthorizationKey string = "Authorization"

const ContentTypeApplicationJson string = "application/json"
const AcceptTypeAppliationSnowflake string = "application/snowflake"
const AcceptTypeAppliationJson string = ContentTypeApplicationJson

const ClientType string = "Go"
const ClientVersion string = "0.1" // TODO: should be updated at build time
const OSVersion string = "0.11"

var UserAgent string = fmt.Sprintf("%s %s", ClientType, ClientVersion)

const StatementTypeIdDml = int64(0x3000)
const StatementTypeIdInsert = StatementTypeIdDml + int64(0x100)
const StatementTypeIdUpdate = StatementTypeIdDml + int64(0x200)
const StatementTypeIdDelete = StatementTypeIdDml + int64(0x300)
const StatementTypeIdMerge = StatementTypeIdDml + int64(0x400)
const StatementTypeIdMultiTableInsert = StatementTypeIdDml + int64(0x500)
