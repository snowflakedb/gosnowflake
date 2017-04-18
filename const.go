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
