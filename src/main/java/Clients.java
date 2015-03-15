import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by art on 15.03.15.
 */
public class Clients {

    // util class
    private Clients(){}

    /**
     * returns a default transport client for localhost with cluster name "elasticsearch" on port 9300
     * @return
     */
    public static Client defaultLocalhostClient() {
        return createClient("localhost", "elasticsearch", 9300);
    }


    public static Client createClient(final String host, final String clusterName, final int port) {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        return new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(host, port));

    }
}
