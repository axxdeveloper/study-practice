package example;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Mapper
public interface EventCountMapper {

    @Select("SELECT EVENT_ID as eventId, SUM(COUNT) as count FROM PUBLIC.EVENT_COUNT WHERE EVENT_TIME = #{eventTime} GROUP BY EVENT_ID ; ")
    List<EventIdCount> findEventIdCountByEventTime(@Param("eventTime") String eventTime);

    @Select("SELECT sum(COUNT) FROM PUBLIC.EVENT_COUNT WHERE EVENT_TIME = #{eventTime} AND EVENT_ID = #{eventId}")
    Integer countByEventTimeEventId(@Param("eventTime") String eventTime, @Param("eventId") String eventId);
}
