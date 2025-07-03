package com.github.jasync.sql.db.pool

import com.github.jasync.sql.db.util.Failure
import com.github.jasync.sql.db.util.Try
import com.github.jasync.sql.db.util.head
import com.github.jasync.sql.db.verifyExceptionInHierarchy
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.spyk
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import java.util.concurrent.ExecutionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.seconds

/**
 * This spec is designed abstract to allow testing of any implementation of AsyncObjectPool, against the common
 * requirements the interface expects.
 *
 * @tparam T the AsyncObjectPool being tested.
 *
 */
abstract class AbstractAsyncObjectPoolSpec<T : AsyncObjectPool<Widget>> {

    private lateinit var pool: T

    protected abstract fun createPool(
        factory: ObjectFactory<Widget> = TestWidgetFactory(),
        conf: PoolConfiguration = PoolConfiguration(10, 4, 10)
    ): T

    @After
    fun closePool() {
        if (::pool.isInitialized) {
            runBlocking {
                pool.close()
            }
        }
    }

    @Test
    fun `the variant of AsyncObjectPool should successfully retrieve and return a Widget `(): Unit = runBlocking {
        pool = createPool()
        val widget = pool.take()

        assertNotNull(widget)

        pool.giveBack(widget)
    }

    @Test(expected = ExecutionException::class)
    fun `the variant of AsyncObjectPool should reject Widgets that did not come from it`(): Unit = runBlocking {
        pool = createPool()
        pool.giveBack(Widget(TestWidgetFactory()))
    }

    @Test
    fun `scale contents`(): Unit = runBlocking {
        val factory = spyk<TestWidgetFactory>()
        pool = createPool(
            factory = factory,
            conf = PoolConfiguration(
                maxObjects = 5,
                maxIdle = 2,
                maxObjectTtl = 5000,
                maxQueueSize = 5,
                validationInterval = 2000

            )
        )

        // "can take up to maxObjects"
        val taken: List<Widget> = (1..5).map { pool.take() }
        assertEquals(5, taken.size)
        (0..4).forEach {
            assertThat(taken[it]).isNotNull()
        }
        // "does not attempt to expire taken items"
        // Wait 3 seconds to ensure idle/maxConnectionTtl check has run at least once
        delay(3000)
        coVerify(exactly = 0) { factory.destroy(any()) }

        // reset(factory) // Considered bad form, but necessary as we depend on previous state in these tests

        // "takes maxObjects back"
        for (it in taken) {
            pool.giveBack(it)
        }

        // "protest returning an item that was already returned"
        verifyExceptionInHierarchy(IllegalStateException::class.java) {
            pool.giveBack(taken.head)
        }

        // "destroy down to maxIdle widgets"
        delay(3000)
        coVerify(exactly = 5) { factory.destroy(any()) }
    }

    private inline fun verifyException(
        exType: Class<out java.lang.Exception>,
        causeType: Class<out java.lang.Exception>? = null,
        body: () -> Unit
    ) {
        try {
            body()
            throw Exception("${exType.simpleName}->${causeType?.simpleName} was not thrown")
        } catch (e: Exception) {
            assertThat(e::class.java).isEqualTo(exType)
            causeType?.let { assertThat(e.cause!!::class.java).isEqualTo(it) }
        }
    }

    @Test
    fun `queue requests after running out`(): Unit = runBlocking {
        pool = createPool(
            conf = PoolConfiguration(
                maxIdle = 4,
                maxObjects = 2,
                maxQueueSize = 1
            )
        )
        val widgets = (1..2).map { pool.take() }
        val future = async { pool.take() }

        // Wait five seconds
        delay(5000)

        val failedFuture = async { pool.take() }

        assertFalse(future.isCompleted)
        verifyException(ExecutionException::class.java, PoolExhaustedException::class.java) {
            failedFuture.await()
        }
        pool.giveBack(widgets.head)
        assertThat(withTimeoutOrNull(5.seconds) { future.await() }).isEqualTo(widgets.head)
    }

    @Test
    fun `refuse to allow take after being closed`(): Unit = runBlocking {
        pool = createPool()
        pool.close()
        verifyExceptionInHierarchy(PoolAlreadyTerminatedException::class.java) {
            pool.take()
        }
    }

    @Test
    fun `allow being closed more than once`(): Unit = runBlocking {
        pool = createPool()
        pool.close()
        pool.close()
    }

    @Test
    fun `destroy a failed widget`(): Unit = runBlocking {
        val factory = spyk<TestWidgetFactory>()
        pool = createPool(factory = factory)
        val widget = pool.take()
        assertThat(widget).isNotNull
        coEvery { factory.validate(widget) } returns Failure(RuntimeException("This is a bad widget!"))
        verifyException(ExecutionException::class.java, RuntimeException::class.java) {
            pool.giveBack(widget)
        }
        factory.destroy(widget)
    }

    @Test
    fun `clean up widgets that die in the pool`(): Unit = runBlocking {
        val factory = spyk<TestWidgetFactory>()
        // Deliberately make it impossible to expire (nearly)
        pool = createPool(
            factory = factory,
            conf = PoolConfiguration(
                maxObjects = 10,
                maxIdle = Long.MAX_VALUE,
                maxQueueSize = 10,
                validationInterval = 2000
            )
        )
        val widget = pool.take()
        assertThat(widget).isNotNull
        pool.giveBack(widget)
        coVerify { factory.validate(widget) }
        coVerify(exactly = 0) { factory.destroy(widget) }
        delay(3000)
        coVerify(atLeast = 2) { factory.validate(widget) }
        coEvery { factory.validate(widget) } returns Failure(RuntimeException("Test Exception, Not an Error"))
        delay(3000)
        coVerify { factory.destroy(widget) }
        pool.take()
        coVerify(exactly = 2) { factory.create() }
    }
}

var idCounter = 0

class Widget(
    val factory: TestWidgetFactory,
    override val creationTime: Long = System.currentTimeMillis()
) :
    PooledObject {
    override val id: String by lazy { "${idCounter++}" }
}

class TestWidgetFactory :
    ObjectFactory<Widget> {

    override suspend fun create(): Widget = Widget(this)

    override suspend fun destroy(item: Widget) {}

    override suspend fun validate(item: Widget): Try<Widget> = Try {
        if (item.factory == this) {
            item
        } else {
            throw IllegalArgumentException("Not our item")
        }
    }
}
