package com.aml.file.pro.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.aml.file.pro.core.efrmsrv.entity.TransactionDetailsEntity;

@Repository
public interface TransactionDetailsRepositry  extends JpaRepository<TransactionDetailsEntity, String>{

}