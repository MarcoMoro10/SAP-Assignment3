package it.unibo.sap.delivery.domain.drone.env;

public class DroneEnvironment extends AbstractEnvironment {

    private static final long TICK_MILLIS = 1000;

    private volatile boolean running = false;
    private volatile long currentTimeMillis;
    private Thread thread;

    public void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofVirtual().name("drone-environment").start(this::loop);
    }

    private void loop() {
        while (running) {
            try {
                Thread.sleep(TICK_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            currentTimeMillis = System.currentTimeMillis();
            notifyEvent(new EnvTimeElapsed(currentTimeMillis));
        }
    }

    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    public long currentTimeMillis() {
        return currentTimeMillis;
    }
}
