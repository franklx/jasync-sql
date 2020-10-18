package com.github.jasync.sql.db.postgresql.column

import com.github.jasync.sql.db.column.ColumnEncoderDecoder
import com.github.jasync.sql.db.exceptions.DateEncoderNotAvailableException
import com.github.jasync.sql.db.general.ColumnData
import com.github.jasync.sql.db.postgresql.messages.backend.PostgreSQLColumnData
import com.github.jasync.sql.db.util.XXX
import io.netty.buffer.ByteBuf
import java.nio.charset.Charset
import java.sql.Timestamp
import java.time.LocalDate
import java.util.Calendar
import java.util.Date
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.TemporalAccessor

object PostgreSQLTimestampEncoderDecoder : ColumnEncoderDecoder {

    private val optionalTimeZone = DateTimeFormatterBuilder()
        .appendPattern("Z").toFormatter()

    private val internalFormatters: List<DateTimeFormatter> = (1..6).map { index ->
        DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendPattern("." + ("S".repeat(index)))
            .appendOptional(optionalTimeZone)
            .toFormatter()
    }

    private val internalFormatterWithoutSeconds = DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendOptional(optionalTimeZone)
        .toFormatter()

    fun formatter() = internalFormatters[5]

    override fun decode(kind: ColumnData, value: ByteBuf, charset: Charset): Any {
        val bytes = ByteArray(value.readableBytes())
        value.readBytes(bytes)

        val text = String(bytes, charset)

        val columnType = kind as PostgreSQLColumnData

        return when (columnType.dataType) {
            ColumnTypes.Timestamp, ColumnTypes.TimestampArray -> {
                selectFormatter(text).parse(text)
            }
            ColumnTypes.TimestampWithTimezoneArray -> {
                selectFormatter(text).parse(text)
            }
            ColumnTypes.TimestampWithTimezone -> {
                if (columnType.dataTypeModifier > 0) {
                    internalFormatters[columnType.dataTypeModifier - 1].parse(text)
                } else {
                    selectFormatter(text).parse(text)
                }
            }
            else -> XXX("should treat ${columnType.dataType}")
        }
    }

    private fun selectFormatter(text: String): DateTimeFormatter {
        return if (text.contains(".")) {
            internalFormatters[5]
        } else {
            internalFormatterWithoutSeconds
        }
    }

    override fun decode(value: String): Any =
        throw UnsupportedOperationException("this method should not have been called")

    override fun encode(value: Any): String {
        return when (value) {
            is Timestamp -> value.toLocalDateTime().format(this.formatter())
            is Date -> LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC).format(this.formatter())
            is Calendar -> LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC).format(this.formatter())
            is LocalDateTime -> this.formatter().format(value)
            is TemporalAccessor -> this.formatter().format(value)
            else -> throw DateEncoderNotAvailableException(value)
        }
    }

    override fun supportsStringDecoding(): Boolean = false
}
