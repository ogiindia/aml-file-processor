package com.aml.file.pro.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.file.pro.core.efrmsrv.entity.AccountDetailsEntity;

import jakarta.data.repository.Repository;

@Repository
public interface AccountDetailsRepositry<T> extends JpaRepository<AccountDetailsEntity, Long>  {

}