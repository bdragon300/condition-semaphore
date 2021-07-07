# Conditional semaphore

Semaphore with support of awaiting a current value by a condition.

## Summary 

Along with methods `Acquire`/`TryAcquire`/`Release`, this semaphore implementation also has a couple of 
additional methods:

* `AwaitLessOrEqual(ctx, n)` -- await the value be less or equal to the given `n`
* `AwaitMoreOrEqual(ctx, n)` -- await the value be more or equal to the given `n`

These methods block a goroutine until the current semaphore value will not meet the condition. They atomically 
check the value between acquires and releases.
