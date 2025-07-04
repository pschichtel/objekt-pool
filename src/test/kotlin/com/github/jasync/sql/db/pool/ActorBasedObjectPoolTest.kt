package com.github.jasync.sql.db.pool

import com.github.jasync.sql.db.util.Try
import com.github.jasync.sql.db.util.isSuccess
import com.github.jasync.sql.db.verifyException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.matches
import org.awaitility.kotlin.untilCallTo
import org.junit.After
import org.junit.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import kotlin.coroutines.suspendCoroutine
import kotlin.test.assertFalse

class ActorBasedObjectPoolTest {

    private val factory = ForTestingMyFactory()
    private val configuration = PoolConfiguration(
        maxObjects = 10,
        maxQueueSize = Int.MAX_VALUE,
        validationInterval = Long.MAX_VALUE,
        maxIdle = Long.MAX_VALUE,
        maxObjectTtl = null
    )
    private lateinit var tested: ActorBasedObjectPool<ForTestingMyWidget>

    private fun createDefaultPool() = ActorBasedObjectPool(
        factory,
        configuration,
        testItemsPeriodically = false
    )

    @After
    fun closePool(): Unit = runBlocking {
        if (::tested.isInitialized) {
            tested.close()
        }
    }

    @Test
    fun `check no take operations can be done after pool is close and connection is cleanup`(): Unit = runBlocking {
        tested = createDefaultPool()
        val widget = tested.take()
        tested.close()
        verifyException(PoolAlreadyTerminatedException::class.java) {
            tested.take()
        }
        assertThat(factory.destroyed).isEqualTo(listOf(widget))
    }

    @Test
    fun `basic take operation`(): Unit = runBlocking {
        tested = createDefaultPool()
        val result = tested.take()
        assertThat(result).isEqualTo(factory.created[0])
        assertThat(result).isEqualTo(factory.validated[0])
    }

    @Test
    fun `basic take operation - when create is stuck should be timeout`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(createTimeout = 10),
            false
        )
        factory.creationStuck = true
        val result = async { tested.take() }
        delay(20)
        tested.testAvailableItems()
        await.untilCallTo { result.getCompletionExceptionOrNull() != null } matches { it == true }
    }

    @Test
    fun `take operation that is waiting in queue - when create is stuck should be timeout`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(maxObjects = 1, queryTimeout = 10),
            false
        )
        // first item is canceled when the create fails
        async { tested.take() }
        val takeSecondItem = async { tested.take() }
        factory.creationStuck = true
        delay(20)
        // third item is not timeout immediately
        val takeThirdItem = async { tested.take() }
        tested.testAvailableItems()
        assertFalse(takeThirdItem.isCompleted)
        await.untilCallTo { takeSecondItem.getCompletionExceptionOrNull() != null } matches { it == true }
        delay(20)
        tested.testAvailableItems()
        await.untilCallTo { takeThirdItem.getCompletionExceptionOrNull() != null } matches { it == true }
    }

    @Test
    fun `basic take operation - when create is little stuck should not be timeout (create timeout is 5 sec)`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(createTimeout = 5000),
            false
        )
        factory.creationStuckTime = 10
        val result = async { tested.take() }
        delay(20)
        tested.testAvailableItems()
        await.untilCallTo { result.isCompleted } matches { it == true }
    }

    @Test
    fun `check items periodically`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(validationInterval = 1000),
            testItemsPeriodically = true
        )
        val result = tested.take()
        tested.giveBack(result)
        delay(1000)
        await.untilCallTo { factory.tested } matches { it?.containsKey(result) == true }
    }

    @Test(expected = Exception::class)
    fun `basic take operation when create failed future should fail`(): Unit = runBlocking {
        tested = createDefaultPool()
        factory.failCreation = true
        tested.take()
    }

    @Test(expected = Exception::class)
    fun `basic take operation when create failed future should fail 2`(): Unit = runBlocking {
        tested = createDefaultPool()
        factory.failCreationFuture = true
        tested.take()
    }

    @Test(expected = Exception::class)
    fun `basic take operation when validation failed future should fail`(): Unit = runBlocking {
        tested = createDefaultPool()
        factory.failValidation = true
        tested.take()
    }

    @Test(expected = Exception::class)
    fun `basic take operation when validation failed future should fail 2`(): Unit = runBlocking {
        tested = createDefaultPool()
        factory.failValidationTry = true
        tested.take()
    }

    @Test
    fun `basic take-return-take operation`(): Unit = runBlocking {
        tested = createDefaultPool()
        val result = tested.take()
        tested.giveBack(result)
        val result2 = tested.take()
        assertThat(result).isEqualTo(result2)
        assertThat(factory.validated).isEqualTo(listOf(result, result, result))
    }

    @Test
    fun `softEviction - basic take-evict-return pool should be empty`(): Unit = runBlocking {
        tested = createDefaultPool()
        val result = tested.take()
        tested.softEvict()
        tested.giveBack(result)
//        assertThat(tested.availableItems).isEmpty()
    }

    @Test
    fun `softEviction - minimum number of objects is maintained, but objects are replaced`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(minIdleObjects = 3),
            false
        )
//        tested.take()
//        await.untilCallTo { tested.availableItemsSize } matches { it == 3 }
//        val availableItems = tested.availableItems
//        tested.softEvict()
//        await.untilCallTo { tested.availableItemsSize } matches { it == 3 }
//        assertThat(tested.availableItems.toSet().intersect(availableItems.toSet())).isEmpty()
    }

    @Test
    fun `test for objects in create eviction in case of softEviction`(): Unit = runBlocking {
        factory.creationStuckTime = 10
        tested = createDefaultPool()
        val itemPromise = async { tested.take() }
        tested.softEvict()
        val item = itemPromise.await()
        tested.giveBack(item)
//        assertThat(tested.availableItemsSize).isEqualTo(0)
    }

    @Test
    fun `take2-return2-take first not validated second is ok should be returned`(): Unit = runBlocking {
        tested = createDefaultPool()
        val result = tested.take()
        val result2 = tested.take()
        tested.giveBack(result)
        tested.giveBack(result2)
        result.isOk = false
        val result3 = tested.take()
        assertThat(result3).isEqualTo(result2)
        assertThat(factory.destroyed).isEqualTo(listOf(result))
    }

    @Test
    fun `basic pool size 1 take2 one should not be completed until 1 returned`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(maxObjects = 1),
            false
        )
        val result = tested.take()
        val result2Future = async { tested.take() }
        assertFalse(result2Future.isCompleted)
        tested.giveBack(result)
        result2Future.await()
    }

    @Test
    fun `basic pool item validation should return to pool after test`(): Unit = runBlocking {
        tested = createDefaultPool()
        val widget = tested.take()
        tested.giveBack(widget)
//        await.untilCallTo { tested.availableItems } matches { it == listOf(widget) }
//        tested.testAvailableItems()
//        await.untilCallTo { factory.tested.size } matches { it == 1 }
//        assertThat(tested.availableItems).isEqualTo(emptyList<ForTestingMyWidget>())
//        factory.tested.getValue(widget).complete(widget)
//        await.untilCallTo { tested.availableItems } matches { it == listOf(widget) }
    }

    @Test
    fun `basic pool item validation should not return to pool after failed test`(): Unit = runBlocking {
        tested = createDefaultPool()
        val widget = tested.take()
        tested.giveBack(widget)
//        await.untilCallTo { tested.availableItems } matches { it == listOf(widget) }
//        tested.testAvailableItems()
//        await.untilCallTo { factory.tested.size } matches { it == 1 }
//        assertThat(tested.availableItems).isEqualTo(emptyList<ForTestingMyWidget>())
//        factory.tested.getValue(widget).completeExceptionally(Exception("failed"))
//        await.untilCallTo { tested.usedItems } matches { it == emptyList<ForTestingMyWidget>() }
//        assertThat(tested.availableItems).isEqualTo(emptyList<ForTestingMyWidget>())
//        assertThat(factory.destroyed).isEqualTo(listOf(widget))
    }

    @Test
    fun `on test items pool should reclaim idle items`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(maxIdle = 10),
            false
        )
        val widget = tested.take()
        tested.giveBack(widget)
        delay(20)
        tested.testAvailableItems()
        await.untilCallTo { factory.destroyed } matches { it == listOf(widget) }
//        assertThat(tested.availableItems).isEmpty()
    }

    @Test
    fun `on take items pool should reclaim items pass ttl`(): Unit = runBlocking {
        tested =
            ActorBasedObjectPool(
                factory,
                configuration.copy(maxObjectTtl = 50),
                false
            )
        val widget = tested.take()
        delay(70)
        tested.giveBack(widget)
        val widget2 = tested.take()
        assertThat(widget).isNotEqualTo(widget2)
        assertThat(factory.created.size).isEqualTo(2)
        assertThat(factory.destroyed[0]).isEqualTo(widget)
    }

    @Test
    fun `on test items pool should reclaim aged-out items`(): Unit = runBlocking {
        tested =
            ActorBasedObjectPool(
                factory,
                configuration.copy(maxObjectTtl = 50),
                false
            )
        val widget = tested.take()
        tested.giveBack(widget)
        delay(70)
        tested.testAvailableItems()
        await.untilCallTo { factory.destroyed } matches { it == listOf(widget) }
//        assertThat(tested.availableItems).isEmpty()
    }

    @Test
    fun `on test of item that last test timeout pool should destroy item`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(testTimeout = 10),
            false
        )
        val widget = tested.take()
        tested.giveBack(widget)
        tested.testAvailableItems()
        delay(20)
        tested.testAvailableItems()
        await.untilCallTo { factory.destroyed } matches { it == listOf(widget) }
//        assertThat(tested.availableItems).isEmpty()
//        assertThat(tested.usedItems).isEmpty()
    }

    @Test
    fun `on query timeout pool should destroy item`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(queryTimeout = 10),
            false,
            extraTimeForTimeoutCompletion = 1
        )
        val widget = tested.take()
        delay(20)
        tested.testAvailableItems()
        await.untilCallTo { factory.destroyed } matches { it == listOf(widget) }
//        assertThat(tested.availableItems).isEmpty()
//        assertThat(tested.usedItems).isEmpty()
    }

    @Test
    fun `when queue is bigger then max waiting, future should fail`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(maxObjects = 1, maxQueueSize = 1),
            false
        )
        tested.take()
        async { tested.take() }
        verifyException(ExecutionException::class.java, PoolExhaustedException::class.java) {
            tested.take()
        }
    }

    @Test
    fun `test for leaks detection - we are taking a widget but lost it so it should be cleaned up`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            ForTestingWeakMyFactory(),
            configuration.copy(maxObjects = 1, maxQueueSize = 1),
            false
        )
        // takeLostItem
        tested.take()
        delay(1000)
        System.gc()
//        await.untilCallTo { tested.usedItemsSize } matches { it == 0 }
//        await.untilCallTo { tested.waitingForItemSize } matches { it == 0 }
//        await.untilCallTo { tested.availableItemsSize } matches { it == 0 }
        System.gc() // to show leak in logging
        delay(1000)
    }

    @Test
    fun `test minIdleObjects - we maintain a minimum number of objects`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(minIdleObjects = 3),
            false
        )
        tested.take()
        delay(20)
//        assertThat(tested.availableItemsSize).isEqualTo(3)
    }

    @Test
    fun `test minIdleObjects - when min = max, we don't go over the total number when returning back`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(maxObjects = 3, minIdleObjects = 3),
            false
        )
        val widget = tested.take()
        delay(20)
        // 3 max, one active, meaning expecting 2 available
//        assertThat(tested.availableItemsSize).isEqualTo(2)
//        tested.giveBack(widget)
//        delay(20)
//        assertThat(tested.availableItemsSize).isEqualTo(3)
    }

    @Test
    fun `test minIdleObjects - cleaned up objects result in more objects being created`(): Unit = runBlocking {
        tested = ActorBasedObjectPool(
            factory,
            configuration.copy(maxObjects = 3, minIdleObjects = 3, maxObjectTtl = 50),
            false
        )
        val widget = tested.take()
        tested.giveBack(widget)
        delay(20)
//        assertThat(tested.availableItemsSize).isEqualTo(3)
//        assertThat(factory.created.size).isEqualTo(3)
//        delay(70)
//        tested.testAvailableItems()
//        await.untilCallTo { tested.availableItemsSize } matches { it == 3 }
//        await.untilCallTo { factory.created.size } matches { it == 6 }
    }
}

private var widgetId = 0

class ForTestingMyWidget(
    var isOk: Boolean = true,
    override val creationTime: Long = System.currentTimeMillis()
) :
    PooledObject {
    override val id: String by lazy { (widgetId++).toString() }
}

class ForTestingWeakMyFactory :
    ObjectFactory<ForTestingMyWidget> {
    override suspend fun create(): ForTestingMyWidget {
        return ForTestingMyWidget()
    }

    override suspend fun destroy(item: ForTestingMyWidget) {
    }

    override suspend fun validate(item: ForTestingMyWidget): Try<ForTestingMyWidget> {
        return Try.just(item)
    }
}

class ForTestingMyFactory :
    ObjectFactory<ForTestingMyWidget> {

    val created = mutableListOf<ForTestingMyWidget>()
    val destroyed = mutableListOf<ForTestingMyWidget>()
    val validated = mutableListOf<ForTestingMyWidget>()
    val tested = mutableMapOf<ForTestingMyWidget, CompletableFuture<ForTestingMyWidget>>()
    var creationStuck: Boolean = false
    var creationStuckTime: Long? = null
    var failCreation: Boolean = false
    var failCreationFuture: Boolean = false
    var failValidation: Boolean = false
    var failValidationTry: Boolean = false

    override suspend fun create(): ForTestingMyWidget {
        if (creationStuck) {
            return suspendCoroutine {
                // never continue
            }
        }
        creationStuckTime?.let {
            delay(it)
            val widget = ForTestingMyWidget()
            created += widget
            return widget
        }
        if (failCreation) {
            throw Exception("failed to create")
        }

        // TODO this should be redundant with failCreation
        if (failCreationFuture) {
            throw Exception("failed to create")
        }
        val widget = ForTestingMyWidget()
        created += widget
        return widget
    }

    override suspend fun destroy(item: ForTestingMyWidget) {
        destroyed += item
    }

    override suspend fun validate(item: ForTestingMyWidget): Try<ForTestingMyWidget> {
        if (failValidation) {
            throw Exception("failed to validate")
        }
        if (failValidationTry || !item.isOk) {
            return Try.raise(Exception("failed to create"))
        }
        validated += item
        return Try.just(item)
    }

    override suspend fun test(item: ForTestingMyWidget): ForTestingMyWidget {
        val completableFuture = CompletableFuture<ForTestingMyWidget>()

        // TODO the original implementation isn't quite clear

        tested += item to completableFuture
        return item
    }
}
