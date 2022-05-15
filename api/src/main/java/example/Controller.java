package example;

import java.time.Instant;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController()
@RequestMapping("/api")
public class Controller {

    private final EventCountService service;

    public Controller(EventCountService service) {
        this.service = service;
    }

    @GetMapping("/currentTime")
    public long time() {
        return Instant.now().toEpochMilli();
    }

    @GetMapping("/counters/time/{eventTime}")
    public @ResponseBody String getByTime(@PathVariable String eventTime) {
        return service.findEventIdCountByEventTime(eventTime);
    }
    
    @GetMapping("/counters/time/{eventTime}/eventId/{eventId}")
    public @ResponseBody Integer countByEventTimeEventId(@PathVariable String eventTime, @PathVariable String eventId) {
        return service.countByEventTimeEventId(eventTime, eventId);
    }

}
