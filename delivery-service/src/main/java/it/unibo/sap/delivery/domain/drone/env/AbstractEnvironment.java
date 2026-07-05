package it.unibo.sap.delivery.domain.drone.env;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractEnvironment {

    private final List<EnvironmentObserver> observers = new CopyOnWriteArrayList<>();

    public void register(final EnvironmentObserver observer) {
        observers.add(observer);
    }

    public void unregister(final EnvironmentObserver observer) {
        observers.remove(observer);
    }

    protected void notifyEvent(final EnvironmentEvent event) {
        for (final EnvironmentObserver observer : observers) {
            observer.onEnvironmentEvent(event);
        }
    }
}
