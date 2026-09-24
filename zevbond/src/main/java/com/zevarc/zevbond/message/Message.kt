package com.zevarc.zevbond.message

data class Message(
    val type: MessageType,
    val data: ByteArray
)