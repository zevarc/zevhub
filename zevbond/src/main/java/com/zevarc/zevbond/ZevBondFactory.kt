package com.zevarc.zevbond

import com.zevarc.zevbond.discovery.WifiP2pLink
import com.zevarc.zevbond.identity.LocalIdentity
import com.zevarc.zevbond.tcp.TcpLink
import kotlinx.coroutines.CoroutineScope

object ZevBondFactory {

    fun create(
        context: android.content.Context,
        scope: CoroutineScope
    ): ZevBond {
        return ZevBondImpl(
            identity = LocalIdentity.from(context),
            p2p = WifiP2pLink(context),
            tcp = TcpLink(scope),
            scope = scope
        )
    }
}
