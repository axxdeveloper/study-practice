package example;

public class EventIdCount {

    private Integer eventId;
    private Long count;

    @Override
    public String toString() {
        return "EventIdCount{" +
                "eventId=" + eventId +
                ", count=" + count +
                '}';
    }

    public void setEventId(Integer eventId) {
        this.eventId = eventId;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getCount() {
        return count;
    }

    public Integer getEventId() {
        return eventId;
    }
}
