import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A limited coupon drop: 100 coupons, one per person.
 *
 * The same load is served by three implementations so you can compare what each one
 * actually hands out. No build tool, no dependencies. One Redis is all it needs.
 *
 *   docker run -d --name coupon-lab-redis -p 6399:6379 redis:7-alpine
 *   java CouponLab.java
 */
public class CouponLab {

    static final String HOST = System.getProperty("redis.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("redis.port", 6399);

    static final int LIMIT = 100;      // coupons available
    static final int USERS = 200;      // distinct users
    static final int RETRY_PCT = 30;   // share of users who press again after no response

    static Resp[] pool;

    // ---------------------------------------------------------------- main

    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println("Limited coupon drop: " + LIMIT + " coupons, one per person");
        System.out.println(USERS + " users, " + RETRY_PCT + "% of them press again after no response");
        System.out.println("Redis " + HOST + ":" + PORT);
        System.out.println();

        List<String> load = buildLoad(true);
        System.out.println("sending " + load.size() + " requests at once");
        System.out.println();

        // one connection per in-flight request, the way a server's pool would hold them
        pool = new Resp[load.size()];
        for (int i = 0; i < pool.length; i++) pool[i] = new Resp();

        Result a = run(buildLoad(true), Impl.INCR_ONLY, "A. INCR only, no per-user check");
        Result b = run(buildLoad(true), Impl.CHECK_THEN_ISSUE, "B. per-user check added");
        Result c = run(buildLoad(true), Impl.ATOMIC_SCRIPT, "C. check and register in one script");

        table(List.of(a, b, c));

        sameCodeTwoTests();
        rollbackDemo();

        for (Resp r : pool) r.close();
    }

    // ------------------------------------------------------------ the load

    /**
     * Builds the request list. A retry is the same user id showing up a second time,
     * because the client never got an answer to the first one.
     */
    static List<String> buildLoad(boolean withRetry) {
        List<String> load = new ArrayList<>();
        for (int i = 0; i < USERS; i++) load.add("u" + i);
        if (withRetry) {
            for (int i = 0; i < USERS; i++) {
                if (i % 100 < RETRY_PCT) load.add("u" + i);        // same person, second press
            }
        } else {
            for (int i = 0; i < USERS * RETRY_PCT / 100; i++) load.add("v" + i);  // different people
        }
        Collections.shuffle(load, new Random(42));
        return load;
    }

    // -------------------------------------------------------------- runner

    enum Impl { INCR_ONLY, CHECK_THEN_ISSUE, ATOMIC_SCRIPT }

    record Result(String name, int issued, int people, int gotTwoOrMore, int failed) {}

    static Result run(List<String> load, Impl impl, String name) throws Exception {
        try (Resp admin = new Resp()) {
            admin.cmd("DEL", "coupon:count", "coupon:winners");
        }

        Queue<String> issued = new ConcurrentLinkedQueue<>();
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(load.size());

        for (int i = 0; i < load.size(); i++) {
            String uid = load.get(i);
            Resp conn = pool[i];
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    if (issue(conn, impl, uid)) issued.add(uid);
                } catch (Exception e) {
                    failed.incrementAndGet();     // never reached Redis. must stay 0
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await(60, TimeUnit.SECONDS);

        Map<String, Integer> per = new HashMap<>();
        for (String u : issued) per.merge(u, 1, Integer::sum);
        int twoOrMore = (int) per.values().stream().filter(v -> v >= 2).count();

        return new Result(name, issued.size(), per.size(), twoOrMore, failed.get());
    }

    /** One coupon request. Returns true when a coupon goes out. */
    static boolean issue(Resp r, Impl impl, String uid) throws IOException {
        switch (impl) {
            case INCR_ONLY -> {
                long n = (Long) r.cmd("INCR", "coupon:count");
                return n <= LIMIT;
            }
            case CHECK_THEN_ISSUE -> {
                long already = (Long) r.cmd("SISMEMBER", "coupon:winners", uid);
                if (already == 1) return false;                       // one per person
                long n = (Long) r.cmd("INCR", "coupon:count");
                if (n > LIMIT) return false;                          // stock left
                r.cmd("SADD", "coupon:winners", uid);
                return true;
            }
            case ATOMIC_SCRIPT -> {
                Object v = r.cmd("EVAL", SCRIPT, "1", "coupon:winners", uid, String.valueOf(LIMIT));
                return ((Long) v) > 0;
            }
        }
        return false;
    }

    static final String SCRIPT = """
            if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then return -2 end
            if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then return -1 end
            return redis.call('SCARD', KEYS[1])
            """;

    // --------------------------------------------------------------- print

    static void table(List<Result> rows) {
        line();
        System.out.printf("%-40s %8s %8s %8s%n", "", "issued", "people", "got 2+");
        line();
        for (Result r : rows) {
            System.out.printf("%-40s %8d %8d %8d%n", r.name(), r.issued(), r.people(), r.gotTwoOrMore());
        }
        line();
        int failed = rows.stream().mapToInt(Result::failed).sum();
        if (failed > 0) System.out.println(failed + " requests never reached Redis. the numbers above are not usable");
        System.out.println();
    }

    static void line() {
        System.out.println("-".repeat(68));
    }

    // ----------------------------------------------- same code, two tests

    /** Implementation B under two different loads. The code does not change. */
    static void sameCodeTwoTests() throws Exception {
        System.out.println("the same implementation B, checked by two tests");
        System.out.println();

        Result t1 = run(buildLoad(false), Impl.CHECK_THEN_ISSUE, "");
        boolean p1 = t1.issued() == LIMIT && t1.gotTwoOrMore() == 0;
        System.out.printf("  %-46s %s   issued %d, got 2+ %d%n",
                "Test 1  " + USERS * (100 + RETRY_PCT) / 100 + " distinct users, all at once",
                p1 ? "PASS" : "FAIL", t1.issued(), t1.gotTwoOrMore());

        Result t2 = run(buildLoad(true), Impl.CHECK_THEN_ISSUE, "");
        boolean p2 = t2.issued() == LIMIT && t2.gotTwoOrMore() == 0;
        System.out.printf("  %-46s %s   issued %d, got 2+ %d%n",
                "Test 2  " + RETRY_PCT + "% of them press again",
                p2 ? "PASS" : "FAIL", t2.issued(), t2.gotTwoOrMore());

        System.out.println();
        System.out.println("  write only Test 1 and this implementation ships");
        System.out.println();
    }

    // ------------------------------------------------------ rollback demo

    /** Inside MULTI, a failing command leaves the earlier one applied. */
    static void rollbackDemo() throws Exception {
        System.out.println("MULTI with a second command that fails");
        System.out.println();
        try (Resp r = new Resp()) {
            r.cmd("DEL", "demo:counter");
            r.cmd("SET", "demo:counter", "10");
            r.cmd("MULTI");
            r.cmd("INCR", "demo:counter");             // succeeds
            r.cmd("LPUSH", "demo:counter", "x");       // wrong type, fails
            Object exec = r.cmd("EXEC");
            System.out.println("  EXEC reply    " + exec);
            System.out.println("  value after   " + r.cmd("GET", "demo:counter"));
            System.out.println();
            System.out.println("  it stays at 11. nothing rolls back");
            System.out.println();
        }
    }

    // ------------------------------------------------- minimal Redis client

    /** Speaks RESP over a socket, so the lab needs no client library. */
    static class Resp implements Closeable {
        private final Socket sock;
        private final OutputStream out;
        private final BufferedInputStream in;

        Resp() throws IOException {
            sock = new Socket(HOST, PORT);
            sock.setTcpNoDelay(true);
            out = new BufferedOutputStream(sock.getOutputStream());
            in = new BufferedInputStream(sock.getInputStream());
        }

        Object cmd(String... args) throws IOException {
            StringBuilder sb = new StringBuilder("*").append(args.length).append("\r\n");
            for (String a : args) {
                byte[] b = a.getBytes(StandardCharsets.UTF_8);
                sb.append("$").append(b.length).append("\r\n").append(a).append("\r\n");
            }
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            return read();
        }

        private Object read() throws IOException {
            int type = in.read();
            if (type == -1) throw new EOFException();
            String head = readLine();
            switch (type) {
                case '+': return head;
                case '-': return "ERR " + head;
                case ':': return Long.parseLong(head);
                case '$': {
                    int len = Integer.parseInt(head);
                    if (len < 0) return null;
                    byte[] buf = in.readNBytes(len);
                    in.readNBytes(2);
                    return new String(buf, StandardCharsets.UTF_8);
                }
                case '*': {
                    int n = Integer.parseInt(head);
                    if (n < 0) return null;
                    List<Object> arr = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) arr.add(read());
                    return arr;
                }
                default: throw new IOException("unknown reply type " + (char) type);
            }
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                if (c == '\r') { in.read(); break; }
                sb.append((char) c);
            }
            return sb.toString();
        }

        public void close() {
            try { sock.close(); } catch (IOException ignored) {}
        }
    }
}
