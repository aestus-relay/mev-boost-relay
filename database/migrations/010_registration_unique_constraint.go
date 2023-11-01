// 010_registration_unique_constraint.go
package migrations

import (
	"github.com/flashbots/mev-boost-relay/database/vars"
	migrate "github.com/rubenv/sql-migrate"
)

// Migration010RegistrationUniqueConstraint adds a unique constraint on
// the fields (pubkey, fee_recipient, gas_limit) in the validator registration table.
// This allows for efficient upserts and tracking of most recent registration.
//
// Some duplicates may exist and must be deleted first.
// This may limit the detail of historical data, but not in most cases.
var Migration010RegistrationUniqueConstraint = &migrate.Migration{
	Id: "010-registration-unique-constraint",
	Up: []string{
		`DELETE FROM ` + vars.TableValidatorRegistration + ` v
		USING (
		    SELECT pubkey, fee_recipient, gas_limit, MAX(timestamp) as max_timestamp
		    FROM ` + vars.TableValidatorRegistration + `
		    GROUP BY pubkey, fee_recipient, gas_limit
		) dupes
		WHERE v.pubkey = dupes.pubkey
		AND v.fee_recipient = dupes.fee_recipient
		AND v.gas_limit = dupes.gas_limit
		AND v.timestamp < dupes.max_timestamp;`,
		`ALTER TABLE ` + vars.TableValidatorRegistration + `
		ADD CONSTRAINT unique_pubkey_fee_gas UNIQUE(pubkey, fee_recipient, gas_limit);`,
	},
	Down: []string{`
		ALTER TABLE ` + vars.TableValidatorRegistration + `
		DROP CONSTRAINT unique_pubkey_fee_gas;
	`},

	DisableTransactionUp:   false,
	DisableTransactionDown: true,
}
