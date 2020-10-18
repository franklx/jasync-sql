package com.github.jasync.sql.db.column

import com.google.common.net.InetAddresses
import java.net.InetAddress

object InetAddressEncoderDecoder : ColumnEncoderDecoder {

    override fun decode(value: String): Any = InetAddresses.forString(value)

    override fun encode(value: Any): String {
        return (value as InetAddress).hostAddress
    }
}
