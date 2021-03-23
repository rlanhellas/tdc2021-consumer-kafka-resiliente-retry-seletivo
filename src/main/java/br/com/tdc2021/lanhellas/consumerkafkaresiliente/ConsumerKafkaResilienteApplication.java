package br.com.tdc2021.lanhellas.consumerkafkaresiliente;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ConsumerKafkaResilienteApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerKafkaResilienteApplication.class, args);
    }

}
