package com.baidu.conf;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/******************************
 * @author : liuyang
 * <p>ProjectName:elasticsearch  </p>
 * @ClassName :  ElasticConfig
 * @date : 2018/11/18 0018
 * @time : 0:41
 * @createTime 2018-11-18 0:41
 * @version : 2.0
 * @description :
 *
 *
 *
 *******************************/

@Configuration
public class ElasticConfig {


    @Bean
    public TransportClient client() throws UnknownHostException {

        InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(
                InetAddress.getByName("localhost"), 9300
        );

        Settings settings = Settings.builder().put("cluster.name", "mrliu").build();

        TransportClient transportClient = new PreBuiltTransportClient(settings);
        transportClient.addTransportAddress(inetSocketTransportAddress);

        return transportClient;

    }


}
