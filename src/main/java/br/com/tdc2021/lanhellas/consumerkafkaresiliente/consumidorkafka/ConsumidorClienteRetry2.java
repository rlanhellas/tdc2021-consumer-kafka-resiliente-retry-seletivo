package br.com.tdc2021.lanhellas.consumerkafkaresiliente.consumidorkafka;

import br.com.tdc2021.lanhellas.consumerkafkaresiliente.entidade.Cliente;
import br.com.tdc2021.lanhellas.consumerkafkaresiliente.repositorio.ClienteRepositorio;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ConsumidorClienteRetry2 extends ConsumidorClienteBase {

    @Value("${app.tempo-retry2-segundos}")
    private long tempoRetry2Segundos;

    protected ConsumidorClienteRetry2(KafkaTemplate<String, Cliente> kafkaTemplate, ClienteRepositorio clienteRepositorio) {
        super(kafkaTemplate, clienteRepositorio);
    }


    @KafkaListener(topics = "${app.topico-cliente-retry2}")
    public void consumir(@Payload Cliente cliente,
                         @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
                         @Header(KafkaHeaders.RECEIVED_TOPIC) String topico,
                         @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
                         Acknowledgment ack) {
        iniciarConsumo(topico, key, cliente, ts, ack);
    }

    @Override
    protected String getTopicoRetry() {
        return null;
    }

    @Override
    protected long getTopicoRetryTempoSegundos() {
        return tempoRetry2Segundos;
    }
}
