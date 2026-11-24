package com.zevarc.zevhub.message

data class Message(
    val type: MessageType,
    val data: ByteArray
)