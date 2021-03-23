package br.com.tdc2021.lanhellas.consumerkafkaresiliente.repositorio;

import br.com.tdc2021.lanhellas.consumerkafkaresiliente.entidade.Cliente;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ClienteRepositorio extends JpaRepository<Cliente, String> {
    long countByNome(String nome);
}
