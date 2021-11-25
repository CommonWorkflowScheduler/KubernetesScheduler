package fonda.scheduler.model;

public class InputParam<T> {

    public final String name;
    public final T value;

    private InputParam() {
        this.name = null;
        this.value = null;
    }

    @Override
    public String toString() {
        return "InputParam{" +
                "name='" + name + '\'' +
                ", value=" + value +
                '}';
    }
}
