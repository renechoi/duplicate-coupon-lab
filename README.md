# duplicate-coupon-lab

A limited coupon drop with a per-user limit written right there in the code, and the same
person still walks away with two. Then the same run with the check and the registration
fused into one operation.

Companion code for the article *원자적인데 틀렸습니다* (engineering).

## Run

```bash
docker run -d --name coupon-lab-redis -p 6399:6379 redis:7-alpine
java CouponLab.java
```

JDK 21 or newer. No build tool, no client library. The lab speaks RESP over a socket, so
Redis is the only thing you need running. Point it elsewhere with `-Dredis.port=6379`.

## What it does

100 coupons for 200 users, one per person. Thirty percent of the users never get an answer
to their first request and press again, so their id shows up twice and the two requests
overlap. That is 260 requests, fired at once.

Three implementations serve the identical load.

| | |
|---|---|
| **A** | `INCR` for the stock count. No per-user check at all. |
| **B** | `SISMEMBER` to enforce one per person, then `INCR`, then `SADD`. |
| **C** | One Lua script that reads `SADD`'s return value, so the check and the registration are the same operation. |

Three numbers come back: coupons issued, people who got one, and people who got two or more.

## Results, 30 runs

```
                                        issued        people         got 2+
A. INCR only, no per-user check            100   86.9 (85-91)   13.1 (9-15)
B. per-user check added                    100   87.4 (84-93)   12.6 (7-16)
C. check and register in one script        100  100.0 (100)      0.0 (0)
```

The stock is exact everywhere. `INCR` is atomic, so the 101st coupon never goes out.

**A and B do not separate.** 86.9 distinct winners before the per-user check, 87.4 after it.
Adding the check changed nothing measurable, because a network round trip sits between the
check and the registration and the same user's two requests walk through it side by side.

C handed 100 coupons to 100 people on every one of the 30 runs.

## The same code, two tests

Implementation B, unchanged, under two loads.

```
Test 1  260 distinct users, all at once      30 of 30 PASS
Test 2  30% of them press again              30 of 30 FAIL
```

A concurrency test written with distinct users lets this implementation through every time.

## MULTI

The lab also runs this, because "just wrap it in MULTI and roll back" is the usual next idea.

```
SET demo:counter 10
MULTI
INCR demo:counter      -> 11
LPUSH demo:counter x   -> WRONGTYPE error
EXEC
GET demo:counter       -> 11
```

The first command stays applied. From the Redis documentation:

> Redis does not support rollbacks of transactions since supporting rollbacks would have a
> significant impact on the simplicity and performance of Redis.

> even when a command fails, all the other commands in the queue are processed

Source: `content/develop/using-commands/transactions.md` in [redis/docs](https://github.com/redis/docs).

## Scope

What this reproduces is **two requests that overlap**. A retry that arrives after the first
one has finished registering does get caught by B. Overlapping is what a double click, a
client timeout retry, and an automatic re-request all produce.

The 30 percent retry share is a chosen parameter, not an observation. Change `RETRY_PCT` in
the source to move it.

Measured on a Mac mini (M4), JDK 21, one Redis 7 container. Your numbers will differ.

## License

MIT
