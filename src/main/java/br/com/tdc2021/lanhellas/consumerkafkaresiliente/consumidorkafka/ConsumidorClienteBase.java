package br.com.tdc2021.lanhellas.consumerkafkaresiliente.consumidorkafka;

import br.com.tdc2021.lanhellas.consumerkafkaresiliente.config.Constantes;
import br.com.tdc2021.lanhellas.consumerkafkaresiliente.entidade.Cliente;
import br.com.tdc2021.lanhellas.consumerkafkaresiliente.repositorio.ClienteRepositorio;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.exception.JDBCConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;

import java.time.Instant;

public abstract class ConsumidorClienteBase {

    @Value("${app.tempo-nack-ms}")
    private long tempoNackMs;

    @Value("${app.topico-deadletter}")
    private String topicoDeadletter;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ClienteRepositorio clienteRepositorio;
    private final KafkaTemplate<String, Cliente> kafkaTemplate;

    protected ConsumidorClienteBase(KafkaTemplate<String, Cliente> kafkaTemplate, ClienteRepositorio clienteRepositorio) {
        this.kafkaTemplate = kafkaTemplate;
        this.clienteRepositorio = clienteRepositorio;
    }

    protected abstract String getTopicoRetry();
    protected abstract long getTopicoRetryTempoSegundos();

    protected void iniciarConsumo(String topico, String key, Cliente cliente, long ts, Acknowledgment ack) {
        try {

            if (topico.contains("-retry")) {
                //delay retry
                var tsAgora = Instant.now().toEpochMilli();
                var diffTs = tsAgora - ts;
                var diffSegundos = diffTs > 1000 ? diffTs / 1000 : 0;
                if (diffSegundos < getTopicoRetryTempoSegundos()){
                    var tempoNack = getTopicoRetryTempoSegundos() - diffSegundos;
                    logger.info("Fazendo NACK de {} segundos no tópico retry {}", tempoNack, topico);
                    ack.nack(tempoNack*1000);
                    return;
                }
            }

            logger.info("#####################################################");
            logger.info("Iniciando consumo do tópico {}, key {}, Cpf Cliente {}", topico, key, cliente.getCpf());
            long countByname = clienteRepositorio.countByNome(cliente.getNome());
            if (countByname > 0) {
                logger.warn("Foram encontrados {} clientes com o nome {}", countByname, cliente.getNome());
                if (!enviarParaRetry(cliente, topico, String.format("Foram encontrado %s clientes com o nome %s", countByname, cliente.getNome()), ts)) {
                    ack.nack(tempoNackMs);
                }else{
                    commit(ack);
                }
            } else {
                clienteRepositorio.save(cliente);
                commit(ack);
            }
        } catch (Exception e) {
            /*
             * Se ocorrer um erro de conexão com a base (ex: timeout por firewall, credenciais inválidas ..) então tentaremos
             * novamente pois não adianta passar para a próxima mensagem, já que todas precisam ser salvas na base.
             * */
            if (e.getCause() instanceof JDBCConnectionException) {
                logger.error("Problemas ao comunicar com a base de dados, tentaremos novamente em 10segundos", e);
                ack.nack(tempoNackMs);
            } else {
                /*
                 * Qualquer outro erro não mapeado, estamos apenas dando commit na mensagem e enviando para o deadletter
                 * */
                logger.error("Erro desconhecido ao tentar salvar", e);
                if (enviarParaDLQ(cliente, topico, e.getMessage())) {
                    commit(ack);
                } else {
                    ack.nack(tempoNackMs);
                }
            }
        }
    }

    private void commit(Acknowledgment ack) {
        ack.acknowledge();
        logger.info("Commit realizado");
    }

    private boolean enviarParaRetry(Cliente cliente, String topicoOriginal, String msgErro, long timestampOriginal) {
        if (getTopicoRetry() == null) {
            return enviarParaDLQ(cliente, topicoOriginal, msgErro);
        }

        var record = new ProducerRecord<String, Cliente>(getTopicoRetry(), cliente);
        record.headers().add(Constantes.KAFKA_HEADER_RETRY_TIMESTAMP_ORIGINAL, String.valueOf(timestampOriginal).getBytes());
        try {
            logger.warn("Enviando para tópico retry: {}", getTopicoRetry());
            kafkaTemplate.send(record).get();
            return true;
        } catch (Exception e) {
            logger.error("Ocorreram erros ao tentar enviar para o tópico de retry", e);
            return false;
        }
    }

    private boolean enviarParaDLQ(Cliente cliente, String topicoOriginal, String msgErro) {
        var record = new ProducerRecord<String, Cliente>(topicoDeadletter, cliente);
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, topicoOriginal.getBytes());
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, msgErro.getBytes());
        try {
            logger.warn("Enviando cliente cpf {} para DLQ no tópico {}", cliente.getCpf(), topicoDeadletter);
            kafkaTemplate.send(record).get();
            return true;
        } catch (Exception e) {
            logger.error("Ocorreram erros ao tentar enviar para o DLQ", e);
            return false;
        }
    }

}
