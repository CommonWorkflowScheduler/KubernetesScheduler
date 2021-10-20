package fonda.scheduler.model;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Quantity;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class NodeWithAlloc implements Comparable<NodeWithAlloc> {

    private String nodeName;

    private BigDecimal current_cpu_usage;

    private BigDecimal current_ram_usage;

    private BigDecimal free_cpu;

    private BigDecimal free_ram;

    private Node node;

    public NodeWithAlloc(Node node) {
        this.node = node;
        this.nodeName = node.getMetadata().getName();
        this.current_ram_usage = new BigDecimal(0);
        this.current_cpu_usage = new BigDecimal(0);
        this.free_ram = new BigDecimal(0);
        this.free_cpu = new BigDecimal(0);

    }

    public void calculateAlloc() {

        Quantity max_cpu = this.node.getStatus().getAllocatable().get("cpu");
        Quantity max_ram = this.node.getStatus().getAllocatable().get("memory");

        this.free_cpu = Quantity.getAmountInBytes(max_cpu).subtract(current_cpu_usage);
        this.free_ram = Quantity.getAmountInBytes(max_ram).subtract(current_ram_usage);

    }

    @Override
    public String toString() {
        return "NodeWithAlloc{" +
                "nodeName='" + nodeName + '\'' +
                ", current_cpu_usage=" + current_cpu_usage +
                ", current_ram_usage=" + current_ram_usage +
                ", free_cpu=" + free_cpu +
                ", free_ram=" + free_ram +
                ", node=" + node +
                '}';
    }

    @Override
    public int compareTo(NodeWithAlloc o) {
        if(getNode().getMetadata().getName().equals(o.getNode().getMetadata().getName())) {
            return 0;
        } else {
            return -1;
        }
    }
}
