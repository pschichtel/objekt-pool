package com.github.jasync.sql.db.pool

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

/**
 *
 * Defines the common interface for sql object pools. These are pools that do not block clients trying to acquire
 * a resource from it. Different than the usual synchronous pool, you **must** return objects back to it manually
 * since it's impossible for the pool to know when the object is ready to be given back. The use method will do that for you automatically
 *
 * @tparam T the objects in the pool
 */

interface AsyncObjectPool<T> {

    /**
     *
     * Returns an object from the pool to the callee , the returned future. If the pool can not create or enqueue
     * requests it will fill the returned Future with a com.github.jasync.sql.db.pool.PoolExhaustedException
     *
     * @return future that will eventually return a usable pool object.
     */
    suspend fun take(): T

    /**
     *
     * Returns an object taken from the pool back to it. This object will become available for another client to use.
     * If the object is invalid or can not be reused for some reason the <<scala.concurrent.Future>> returned will contain
     * the error that prevented this object of being added back to the pool. The object is then discarded from the pool.
     *
     * @param item
     * @return
     */
    suspend fun giveBack(item: T)

    /**
     * Mark all objects in the pool as invalid. Objects will be evicted when not in use.
     */
    suspend fun softEvict()

    /**
     *
     * Closes this pool and future calls to **take** will cause the Future to raise an
     * com.github.jasync.sql.db.pool.PoolAlreadyTerminatedException.
     *
     * @return
     */
    suspend fun close()
}

/**
 *
 * Retrieve and use an object from the pool for a single computation, returning it when the operation completes.
 *
 * @param block function that uses the object
 * @return result of block
 */
suspend fun <T, R> AsyncObjectPool<T>.use(block: (T) -> R): R {
    val item = take()
    try {
        return block(item)
    } finally {
        withContext(NonCancellable) {
            giveBack(item)
        }
    }
}
