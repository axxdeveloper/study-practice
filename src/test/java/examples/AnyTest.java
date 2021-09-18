package examples;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import examples.protobuf.Request;
import org.junit.Assert;
import org.junit.Test;

public class AnyTest {

    @Test
    public void testCreateRequest() throws InvalidProtocolBufferException {
        Request.CreateRequest createRequest = Request.CreateRequest.newBuilder().setName("name").build();
        byte[] ary = createRequest.toByteArray();

        // send to remove server, remote server need to parse the byte array
        Request.CreateRequest receive = Request.CreateRequest.parseFrom(ary);
        Assert.assertEquals(receive.getName(), createRequest.getName());
    }

    @Test
    public void testAnyCreateRequest() throws InvalidProtocolBufferException {
        Request.CreateRequest createRequest = Request.CreateRequest.newBuilder().setName("name").build();
        byte[] ary = Any.pack(createRequest).toByteArray();
        
        // send to remove server, remote server need to parse the byte array 
        Any any = Any.parseFrom(ary);
        Assert.assertTrue(any.is(Request.CreateRequest.class));
        Request.CreateRequest unpack = any.unpack(Request.CreateRequest.class);
        Assert.assertEquals(unpack.getName(), createRequest.getName());
    }

}
