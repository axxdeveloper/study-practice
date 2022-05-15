package example;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.springframework.stereotype.Service;

@Service
public class EventCountService {

    private final EventCountMapper eventCountMapper;

    public EventCountService(EventCountMapper eventCountMapper) {
        this.eventCountMapper = eventCountMapper;
    }
    
    public String findEventIdCountByEventTime(String eventTime) {
        JsonArray result = new JsonArray();
        eventCountMapper.findEventIdCountByEventTime(eventTime)
                .stream().forEach(e -> {
                    JsonObject o = new JsonObject();
                    o.addProperty(String.valueOf(e.getEventId()), e.getCount());
                    result.add(o);
                });
        return result.toString();
    }

    public Integer countByEventTimeEventId(String eventTime, String eventId) {
        return eventCountMapper.countByEventTimeEventId(eventTime, eventId);
    }
}
