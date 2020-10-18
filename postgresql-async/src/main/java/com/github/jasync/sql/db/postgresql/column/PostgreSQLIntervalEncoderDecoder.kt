package com.github.jasync.sql.db.postgresql.column

import com.github.jasync.sql.db.column.ColumnEncoderDecoder
import com.github.jasync.sql.db.exceptions.DateEncoderNotAvailableException
import java.time.Duration
import java.time.Period

object PostgreSQLIntervalEncoderDecoder : ColumnEncoderDecoder {

    override fun encode(value: Any): String {
        return when (value) {
            is Period -> value.toString()
            is Duration -> value.toString() // default to ISO8601
            else -> throw DateEncoderNotAvailableException(value)
        }
    }

    /* This supports all positive intervals, and intervalstyle of postgres_verbose, and iso_8601 perfectly.
     * If intervalstyle is set to postgres or sql_standard, some negative intervals may be rejected.
     */
    override fun decode(value: String): Period {
        return if (value.isEmpty()) { /* huh? */
            Period.ZERO
        } else {
            Period.parse(value)
        }
    }
}
