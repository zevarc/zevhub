package com.zevarc.zevhub

import com.zevarc.zevhub.discovery.WifiP2pLink
import com.zevarc.zevhub.identity.LocalIdentity
import com.zevarc.zevhub.tcp.TcpLink
import kotlinx.coroutines.CoroutineScope

object ZevHubFactory {

    fun create(
        context: android.content.Context,
        scope: CoroutineScope
    ): ZevHub {
        return ZevHubImpl(
            identity = LocalIdentity.from(context),
            p2p = WifiP2pLink(context),
            tcp = TcpLink(scope),
            scope = scope
        )
    }
}
