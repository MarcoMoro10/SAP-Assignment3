package it.unibo.sap.delivery.domain.drone.agent;

import it.unibo.sap.delivery.domain.drone.env.EnvironmentEvent;
import it.unibo.sap.delivery.domain.drone.env.EnvironmentObserver;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BasicAgentArch implements Runnable, EnvironmentObserver {

    private final String name;
    private final BlockingQueue<Percept> perceptQueue = new LinkedBlockingQueue<>();
    private final Queue<Action> actionQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean running = false;
    private Thread thread;

    protected BasicAgentArch(final String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public void onEnvironmentEvent(final EnvironmentEvent event) {
        perceptQueue.offer(new Percept(event, System.currentTimeMillis()));
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofVirtual().name(name).start(this);
    }

    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    protected void scheduleAction(final Action action) {
        actionQueue.add(action);
    }

    @Override
    public void run() {
        System.out.println("[" + name + "] INIT");
        init();
        while (running) {
            final Percept percept;
            try {
                percept = perceptQueue.take();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            System.out.println("[" + name + "] SENSE " + percept.event());
            sense(percept);
            System.out.println("[" + name + "] PLAN");
            plan();
            System.out.println("[" + name + "] ACT (" + actionQueue.size() + " action/s)");
            act();
        }
        System.out.println("[" + name + "] STOPPED");
    }

    protected void act() {
        Action action;
        while ((action = actionQueue.poll()) != null) {
            action.execute();
        }
    }

    protected abstract void init();

    protected abstract void sense(Percept percept);

    protected abstract void plan();
}
