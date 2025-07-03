package com.github.jasync.sql.db.pool

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class ActorBasedAsyncObjectPoolSpec : AbstractAsyncObjectPoolSpec<ActorBasedObjectPool<Widget>>() {

    override fun createPool(factory: ObjectFactory<Widget>, conf: PoolConfiguration): ActorBasedObjectPool<Widget> =
        ActorBasedObjectPool(factory, conf, true)

    @Test
    fun `SingleThreadedAsyncObjectPool should successfully record a closed state`() {
        val p = createPool()
        assertThat(p.closed).isFalse()
        runBlocking {
            p.close()
            assertThat(p.closed).isTrue()
        }
    }
}
