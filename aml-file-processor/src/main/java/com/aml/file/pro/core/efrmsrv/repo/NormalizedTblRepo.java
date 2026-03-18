package com.aml.file.pro.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.aml.file.pro.core.efrmsrv.entity.NormalizedTblEntity;

@Repository
public interface NormalizedTblRepo extends JpaRepository<NormalizedTblEntity, String> {

}