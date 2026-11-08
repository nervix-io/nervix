package io.nervix.client.suite.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** A bounded opaque-byte relay: cut ends all its sessions; restore reuses the same public address. */
public final class Relay implements AutoCloseable {
    private final URI target;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor workers = new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(32));
    private final int port;
    private ServerSocket listener;
    private Thread accepting;
    private volatile CountDownLatch replies = new CountDownLatch(0);

    public Relay(String target) throws IOException {
        this.target = URI.create(target);
        listener = listen(0);
        port = listener.getLocalPort();
        startAccepting();
    }

    private static ServerSocket listen(int port) throws IOException {
        var socket = new ServerSocket();
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress("127.0.0.1", port));
        return socket;
    }

    public String uri() { return target.getScheme() + "://127.0.0.1:" + port; }

    private void startAccepting() {
        ServerSocket current = listener;
        accepting = new Thread(() -> {
            while (!current.isClosed()) {
                Socket incoming = null;
                Socket upstream = null;
                try {
                    incoming = current.accept();
                    upstream = new Socket();
                    upstream.connect(new InetSocketAddress(target.getHost(), target.getPort()), 30_000);
                    incoming.setTcpNoDelay(true);
                    upstream.setTcpNoDelay(true);
                    connections.add(incoming);
                    connections.add(upstream);
                    Socket input = incoming;
                    Socket output = upstream;
                    workers.execute(() -> pump(input, output, false));
                    workers.execute(() -> pump(output, input, true));
                } catch (IOException | java.util.concurrent.RejectedExecutionException failure) {
                    closeSocket(incoming);
                    closeSocket(upstream);
                    if (!current.isClosed()) throw new IllegalStateException("fixture relay failed", failure);
                }
            }
        }, "jvm-suite-relay");
        accepting.start();
    }

    private void pump(Socket source, Socket destination, boolean response) {
        try {
            var input = source.getInputStream();
            var output = destination.getOutputStream();
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                if (response && !replies.await(120, TimeUnit.SECONDS))
                    throw new AssertionError("reply gate was not released within its bound");
                output.write(buffer, 0, count);
            }
        } catch (IOException closed) {
            // A cut or either peer ending closes both directions of this test connection.
        } catch (InterruptedException stopping) {
            Thread.currentThread().interrupt();
        } finally {
            closeSocket(source);
            closeSocket(destination);
            connections.remove(source);
            connections.remove(destination);
        }
    }

    private static void closeSocket(Socket socket) {
        if (socket == null) return;
        try { socket.close(); }
        catch (IOException alreadyEnded) { /* The fixture no longer borrows this connection. */ }
    }

    public void cut() {
        resumeReplies();
        try {
            listener.close();
            accepting.join(30_000);
            if (accepting.isAlive()) throw new AssertionError("relay accept did not end");
            connections.forEach(Relay::closeSocket);
            connections.clear();
        } catch (IOException failure) {
            throw new IllegalStateException("relay listener did not close", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while ending relay", interrupted);
        }
    }

    public void restore() throws IOException {
        listener = listen(port);
        startAccepting();
    }

    public void pauseReplies() { replies = new CountDownLatch(1); }

    public void resumeReplies() { replies.countDown(); }

    @Override public void close() {
        cut();
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) throw new AssertionError("relay workers did not end");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while ending relay workers", interrupted);
        }
    }
}
